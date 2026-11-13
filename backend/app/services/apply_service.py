from __future__ import annotations

import json
from typing import Any

from app.config import FIXTURES_DIR
from app.db import connect
from app.engines.apply_rules import classify_event
from app.engines.parse_change import parse_change_fixture
from app.modules import schema_evolve_event as evolve
from app.repositories import cursors as cursor_repo
from app.repositories import ledger as ledger_repo
from app.repositories import quarantine as quarantine_repo
from app.repositories import runs as runs_repo
from app.repositories import schema_registry
from app.repositories import settings as settings_repo
from app.repositories import sink as sink_repo
from app.repositories import streams as streams_repo

PHASE_PRE = "pre_evolution"
PHASE_POST = "post_evolution"
PHASE_MARKER = "evolution_marker"

DISPOSITION_APPLIED = "applied"
DISPOSITION_DUPLICATE = "skipped_duplicate"
DISPOSITION_GAP = "quarantine_gap"
DISPOSITION_EVOLVE = "schema_evolve"
DISPOSITION_MAPPING_QUARANTINE = "quarantined_mapping"
DISPOSITION_MAPPING_SKIP = "skipped_mapping"


def preview_stream(stream_id: int) -> dict[str, Any]:
    conn = connect()
    try:
        stream = streams_repo.get_stream(conn, stream_id)
        if not stream:
            raise ValueError("stream not found")
        path = FIXTURES_DIR / stream["fixture_path"]
        events = parse_change_fixture(path)
        evolution = evolve.detect_evolution(
            events, base_version=int(stream.get("base_schema_version") or 1)
        )
        return {
            "stream": stream,
            "count": len(events),
            "events": events[:50],
            "evolution": evolution,
        }
    finally:
        conn.close()


def _merged_points(
    conn, stream_id: int, base_version: int, detected: dict[str, Any]
) -> list[dict[str, Any]]:
    """夹具检测点与手工登记点合并；同 seq 时手工登记优先。"""
    manual = {
        int(p["seq"]): {
            "seq": int(p["seq"]),
            "from_version": int(p["from_version"]),
            "to_version": int(p["to_version"]),
            "source": str(p["source"]),
            "note": p.get("note"),
        }
        for p in schema_registry.list_points(conn, stream_id)
        if str(p["source"]) == "manual"
    }
    merged = {int(p["seq"]): p for p in detected["points"]}
    merged.update(manual)
    points = [merged[seq] for seq in sorted(merged)]
    # 重新计算链：手工点可能跨多个版本，后续点的 from 以前一个点为基准。
    current = base_version
    out: list[dict[str, Any]] = []
    for point in points:
        item = dict(point)
        if item["from_version"] != current and item["source"] != "manual":
            item["from_version"] = current
        out.append(item)
        current = int(item["to_version"])
    return out


def _resolve_rule(
    conn, stream_id: int, from_version: int, to_version: int
) -> dict[str, Any] | None:
    if from_version == to_version:
        return {"from_version": from_version, "to_version": to_version, "fields": []}
    return schema_registry.get_mapping(conn, stream_id, from_version, to_version)


def run_apply(stream_id: int) -> dict[str, Any]:
    conn = connect()
    try:
        stream = streams_repo.get_stream(conn, stream_id)
        if not stream:
            raise ValueError("stream not found")
        if not stream["enabled"]:
            raise ValueError("stream disabled")

        cursor_before = int(stream["apply_cursor"] or 0)
        base_version = int(stream.get("base_schema_version") or 1)
        mapping_policy = str(stream.get("mapping_failure_policy") or "quarantine")
        if mapping_policy not in evolve.MAPPING_FAILURE_POLICIES:
            mapping_policy = evolve.MAPPING_FAILURE_QUARANTINE
        gap_policy = settings_repo.get(conn, "gap_policy", "stop_quarantine") or "stop_quarantine"
        run_id = runs_repo.create_run(conn, stream_id, cursor_before)
        path = FIXTURES_DIR / stream["fixture_path"]

        try:
            events = parse_change_fixture(path)
            detected = evolve.detect_evolution(events, base_version=base_version)
        except Exception as exc:  # noqa: BLE001
            runs_repo.finish_run(
                conn,
                run_id,
                status="failed",
                loaded=0,
                applied=0,
                quarantined=0,
                skipped=0,
                cursor_after=cursor_before,
                report={"error": str(exc)},
                schema={"final_schema_version": base_version},
            )
            conn.commit()
            raise

        points = _merged_points(conn, stream_id, base_version, detected)
        schema_registry.persist_detected_points(conn, stream_id, detected["points"])
        rules = {
            (int(p["from_version"]), int(p["to_version"])): _resolve_rule(
                conn, stream_id, int(p["from_version"]), int(p["to_version"])
            )
            for p in points
        }

        loaded = len(events)
        dispositions: list[str] = []
        first_point_seq = int(points[0]["seq"]) if points else None
        cursor = cursor_before
        stopped = False

        for event in events:
            seq = int(event["seq"])
            phase = PHASE_POST if first_point_seq is not None and seq >= first_point_seq else PHASE_PRE
            active_point = evolve.point_for_seq(points, seq)
            version_at_seq = int(active_point["to_version"]) if active_point else base_version

            already = cursor_repo.is_applied(conn, stream_id, seq)
            decision = classify_event(
                event=event,
                cursor=cursor,
                already_applied=already,
                gap_policy=gap_policy,
            )

            if decision == DISPOSITION_DUPLICATE:
                ledger_repo.insert_ledger(
                    conn,
                    stream_id=stream_id,
                    run_id=run_id,
                    event=event,
                    disposition=decision,
                    schema_version=version_at_seq,
                    evolution_phase=(
                        PHASE_MARKER if event["op"] == evolve.EVOLVE_OP else phase
                    ),
                )
                dispositions.append(decision)
                continue

            if decision == DISPOSITION_GAP:
                ledger_repo.insert_ledger(
                    conn,
                    stream_id=stream_id,
                    run_id=run_id,
                    event=event,
                    disposition=decision,
                    schema_version=version_at_seq,
                    evolution_phase=phase,
                )
                quarantine_repo.insert(
                    conn,
                    stream_id=stream_id,
                    run_id=run_id,
                    seq=seq,
                    entity_key=event["entity_key"],
                    reason="gap_before_seq",
                    payload=event.get("payload") or {},
                )
                dispositions.append(decision)
                stopped = True
                break

            # ---- 演化控制事件：只登记/推进游标，不写 Sink ----
            if event["op"] == evolve.EVOLVE_OP:
                ledger_repo.insert_ledger(
                    conn,
                    stream_id=stream_id,
                    run_id=run_id,
                    event=event,
                    disposition=DISPOSITION_EVOLVE,
                    schema_version=version_at_seq,
                    evolution_phase=PHASE_MARKER,
                    mapping={
                        "from_version": int(active_point["from_version"]) if active_point else base_version,
                        "to_version": version_at_seq,
                        "source": active_point.get("source") if active_point else None,
                    },
                )
                cursor_repo.mark_applied(conn, stream_id, seq, run_id)
                cursor = seq
                streams_repo.set_cursor(conn, stream_id, cursor)
                dispositions.append(DISPOSITION_EVOLVE)
                continue

            if phase == PHASE_PRE:
                if event["op"] == "upsert":
                    sink_repo.upsert(
                        conn,
                        stream_id=stream_id,
                        entity_key=event["entity_key"],
                        payload=event.get("payload") or {},
                        seq=seq,
                        event_ts=event["event_ts"],
                        schema_version=base_version,
                    )
                else:
                    sink_repo.delete(conn, stream_id=stream_id, entity_key=event["entity_key"])
                cursor_repo.mark_applied(conn, stream_id, seq, run_id)
                cursor = seq
                streams_repo.set_cursor(conn, stream_id, cursor)
                ledger_repo.insert_ledger(
                    conn,
                    stream_id=stream_id,
                    run_id=run_id,
                    event=event,
                    disposition=DISPOSITION_APPLIED,
                    schema_version=base_version,
                    evolution_phase=PHASE_PRE,
                    mapping={"compat_policy": evolve.COMPAT_POLICY_PRE_EVOLUTION,
                             "renamed_fields": []},
                )
                dispositions.append(DISPOSITION_APPLIED)
                continue

            # ---- 演化后：规范化后入账/应用 ----
            assert active_point is not None  # first_point_seq 已保证
            rule = rules.get((int(active_point["from_version"]), int(active_point["to_version"])))
            if event["op"] == "delete":
                # delete 不写 payload 到 Sink，按 entity_key 删除，无需字段映射。
                normalised = {
                    "ok": True,
                    "payload": event.get("payload") or {},
                    "missing_fields": [],
                    "renamed_fields": [],
                    "reason": None,
                }
                mapping_info = {
                    "from_version": int(active_point["from_version"]),
                    "to_version": int(active_point["to_version"]),
                    "rule_configured": rule is not None,
                    "renamed_fields": [],
                    "missing_fields": [],
                    "ok": True,
                    "reason": None,
                    "compat_policy": "delete_by_key_no_payload",
                }
            else:
                normalised = evolve.normalize_payload(event.get("payload") or {}, rule)
                mapping_info = {
                    "from_version": int(active_point["from_version"]),
                    "to_version": int(active_point["to_version"]),
                    "rule_configured": rule is not None,
                    "renamed_fields": normalised["renamed_fields"],
                    "missing_fields": normalised["missing_fields"],
                    "ok": normalised["ok"],
                    "reason": normalised["reason"],
                }

            if not normalised["ok"]:
                disposition = (
                    DISPOSITION_MAPPING_QUARANTINE
                    if mapping_policy == evolve.MAPPING_FAILURE_QUARANTINE
                    else DISPOSITION_MAPPING_SKIP
                )
                ledger_repo.insert_ledger(
                    conn,
                    stream_id=stream_id,
                    run_id=run_id,
                    event=event,
                    disposition=disposition,
                    schema_version=version_at_seq,
                    evolution_phase=PHASE_POST,
                    mapping=mapping_info,
                )
                if mapping_policy == evolve.MAPPING_FAILURE_QUARANTINE:
                    quarantine_repo.insert(
                        conn,
                        stream_id=stream_id,
                        run_id=run_id,
                        seq=seq,
                        entity_key=event["entity_key"],
                        reason=str(normalised["reason"] or "schema_mapping_failed"),
                        payload={
                            "raw_payload": event.get("payload") or {},
                            "missing_fields": normalised["missing_fields"],
                            "mapping": mapping_info,
                        },
                    )
                # 游标仍推进：失败事件已显式隔离/跳过并留痕，避免卡住整条流。
                cursor_repo.mark_applied(conn, stream_id, seq, run_id)
                cursor = seq
                streams_repo.set_cursor(conn, stream_id, cursor)
                dispositions.append(disposition)
                continue

            payload = normalised["payload"]
            if event["op"] == "upsert":
                sink_repo.upsert(
                    conn,
                    stream_id=stream_id,
                    entity_key=event["entity_key"],
                    payload=payload,
                    seq=seq,
                    event_ts=event["event_ts"],
                    schema_version=version_at_seq,
                )
            else:
                sink_repo.delete(conn, stream_id=stream_id, entity_key=event["entity_key"])
            cursor_repo.mark_applied(conn, stream_id, seq, run_id)
            cursor = seq
            streams_repo.set_cursor(conn, stream_id, cursor)
            ledger_repo.insert_ledger(
                conn,
                stream_id=stream_id,
                run_id=run_id,
                event={**event, "payload": payload},
                disposition=DISPOSITION_APPLIED,
                schema_version=version_at_seq,
                evolution_phase=PHASE_POST,
                mapping=mapping_info,
            )
            dispositions.append(DISPOSITION_APPLIED)

        final_version = (
            int(evolve.point_for_seq(points, cursor)["to_version"])
            if evolve.point_for_seq(points, cursor)
            else base_version
        )
        status = "partial" if stopped else "succeeded"
        # 账本/隔离/Sink 同源对账：所有计数从本次 run 的真实行派生。
        ledger_counts = _ledger_counts(conn, run_id)
        quarantine_counts = _quarantine_counts(conn, run_id)
        sink_versions = _sink_schema_versions(conn, stream_id)
        derived = _derive_counts(conn, run_id, ledger_counts)
        pre_count = derived["pre_evolution_count"]
        post_count = derived["post_evolution_count"]
        marker_count = derived["marker_count"]
        mapped_count = derived["mapped_count"]
        mapping_failed_count = derived["mapping_failed_count"]
        applied = derived["applied"]
        quarantined = derived["quarantined"]
        skipped = derived["skipped"]
        mapping_failures = _mapping_failures(conn, run_id)
        inconsistencies = _consistency_issues(
            ledger_counts=ledger_counts,
            total_rows=derived["total_rows"],
            pre_count=pre_count,
            post_count=post_count,
            marker_count=marker_count,
            applied=applied,
            quarantined=quarantined,
            skipped=skipped,
            mapped_count=mapped_count,
            mapping_failed_count=mapping_failed_count,
            post_duplicate_count=derived["post_duplicate_count"],
            quarantine_counts=quarantine_counts,
            mapping_policy=mapping_policy,
        )
        evolution_report = {
            "base_schema_version": base_version,
            "final_schema_version": final_version,
            "planned_final_version": int(detected["final_version"]) if not points else int(points[-1]["to_version"]),
            "points": points,
            "detected_points": detected["points"],
            "mapping_rules": [
                {
                    "from_version": int(p["from_version"]),
                    "to_version": int(p["to_version"]),
                    "configured": rules.get((int(p["from_version"]), int(p["to_version"]))) is not None,
                    "fields": (rules.get((int(p["from_version"]), int(p["to_version"]))) or {}).get("fields", []),
                }
                for p in points
            ],
            "pre_evolution_count": pre_count,
            "post_evolution_count": post_count,
            "marker_count": marker_count,
            "mapped_count": mapped_count,
            "mapping_failed_count": mapping_failed_count,
            "post_duplicate_count": derived["post_duplicate_count"],
            "mapping_failures": mapping_failures,
            "mapping_failure_policy": mapping_policy,
            "pre_evolution_policy": evolve.COMPAT_POLICY_PRE_EVOLUTION,
            "ledger_counts": ledger_counts,
            "quarantine_counts": quarantine_counts,
            "sink_schema_versions": sink_versions,
            "consistent": not inconsistencies,
            "inconsistencies": inconsistencies,
        }
        report = {
            "stream_id": stream_id,
            "gap_policy": gap_policy,
            "dispositions": dispositions,
            "stopped_on_gap": stopped,
            "sink_count_after": sink_repo.count_sink(conn),
            "quarantine_count_after": quarantine_repo.count_quarantine(conn),
            "evolution": evolution_report,
        }
        runs_repo.finish_run(
            conn,
            run_id,
            status=status,
            loaded=loaded,
            applied=applied,
            quarantined=quarantined,
            skipped=skipped,
            cursor_after=cursor,
            report=report,
            schema={
                "pre_evolution_count": pre_count,
                "post_evolution_count": post_count,
                "mapped_count": mapped_count,
                "mapping_failed_count": mapping_failed_count,
                "final_schema_version": final_version,
            },
        )
        conn.commit()
        run = runs_repo.get_run(conn, run_id) or {}
        run["sink_schema_versions"] = _sink_schema_versions(conn, stream_id)
        return run
    finally:
        conn.close()


def _sink_schema_versions(conn, stream_id: int) -> dict[str, int]:
    rows = conn.execute(
        "SELECT schema_version, COUNT(*) c FROM sink_records WHERE stream_id=? GROUP BY schema_version",
        (stream_id,),
    ).fetchall()
    return {str(int(r["schema_version"] or 1)): int(r["c"]) for r in rows}


def _ledger_counts(conn, run_id: int) -> dict[str, int]:
    rows = conn.execute(
        "SELECT evolution_phase, COUNT(*) c FROM change_ledger WHERE run_id=? GROUP BY evolution_phase",
        (run_id,),
    ).fetchall()
    counts = {PHASE_PRE: 0, PHASE_POST: 0, PHASE_MARKER: 0}
    for r in rows:
        if r["evolution_phase"]:
            counts[str(r["evolution_phase"])] = int(r["c"])
    return counts


def _quarantine_counts(conn, run_id: int) -> dict[str, int]:
    rows = conn.execute(
        "SELECT reason, COUNT(*) c FROM quarantine WHERE run_id=? GROUP BY reason", (run_id,)
    ).fetchall()
    mapping = sum(
        int(r["c"]) for r in rows if str(r["reason"]).startswith("schema_mapping")
    )
    return {
        "gap": sum(int(r["c"]) for r in rows if str(r["reason"]) == "gap_before_seq"),
        "mapping": mapping,
        "total": sum(int(r["c"]) for r in rows),
    }


def _derive_counts(
    conn, run_id: int, ledger_counts: dict[str, int]
) -> dict[str, int]:
    """从本次 run 的账本行派生报告计数，保证与账本/隔离/Sink 同源。"""
    rows = conn.execute(
        "SELECT disposition, evolution_phase FROM change_ledger WHERE run_id=?",
        (run_id,),
    ).fetchall()
    post_applied = sum(
        1
        for r in rows
        if r["evolution_phase"] == PHASE_POST and r["disposition"] == DISPOSITION_APPLIED
    )
    mapping_failed = sum(
        1
        for r in rows
        if r["disposition"]
        in (DISPOSITION_MAPPING_QUARANTINE, DISPOSITION_MAPPING_SKIP)
    )
    post_duplicate = sum(
        1
        for r in rows
        if r["evolution_phase"] == PHASE_POST and r["disposition"] == DISPOSITION_DUPLICATE
    )
    marker_processed = sum(1 for r in rows if r["disposition"] == DISPOSITION_EVOLVE)
    applied = sum(1 for r in rows if r["disposition"] == DISPOSITION_APPLIED)
    quarantined = sum(
        1
        for r in rows
        if r["disposition"] in (DISPOSITION_GAP, DISPOSITION_MAPPING_QUARANTINE)
    )
    skipped = sum(
        1
        for r in rows
        if r["disposition"] in (DISPOSITION_DUPLICATE, DISPOSITION_MAPPING_SKIP)
    )
    return {
        "pre_evolution_count": ledger_counts[PHASE_PRE],
        "post_evolution_count": ledger_counts[PHASE_POST],
        "marker_count": marker_processed,
        "marker_phase_rows": ledger_counts[PHASE_MARKER],
        "mapped_count": post_applied,
        "mapping_failed_count": mapping_failed,
        "post_duplicate_count": post_duplicate,
        "applied": applied,
        "quarantined": quarantined,
        "skipped": skipped,
        "total_rows": len(rows),
    }


def _mapping_failures(conn, run_id: int) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT seq, entity_key, disposition, mapping_json
        FROM change_ledger
        WHERE run_id=? AND evolution_phase=? AND disposition IN (?,?)
        ORDER BY seq
        """,
        (run_id, PHASE_POST, DISPOSITION_MAPPING_QUARANTINE, DISPOSITION_MAPPING_SKIP),
    ).fetchall()
    failures = []
    for r in rows:
        mapping = json.loads(r["mapping_json"] or "{}")
        failures.append(
            {
                "seq": int(r["seq"]),
                "entity_key": r["entity_key"],
                "reason": mapping.get("reason"),
                "missing_fields": mapping.get("missing_fields") or [],
                "disposition": r["disposition"],
            }
        )
    return failures


def _consistency_issues(
    *,
    ledger_counts: dict[str, int],
    total_rows: int,
    pre_count: int,
    post_count: int,
    marker_count: int,
    applied: int,
    quarantined: int,
    skipped: int,
    mapped_count: int,
    mapping_failed_count: int,
    post_duplicate_count: int,
    quarantine_counts: dict[str, int],
    mapping_policy: str,
) -> list[str]:
    """报告数字必须与账本行数、隔离行数逐项对得上。"""
    phase_pre = ledger_counts[PHASE_PRE]
    phase_post = ledger_counts[PHASE_POST]
    issues: list[str] = []
    if phase_pre != pre_count:
        issues.append(f"pre_evolution_count {pre_count} != ledger pre rows {phase_pre}")
    if phase_post != post_count:
        issues.append(f"post_evolution_count {post_count} != ledger post rows {phase_post}")
    # 演化后事件 = 映射成功 + 映射失败 + 重复序未再处理
    if post_count != mapped_count + mapping_failed_count + post_duplicate_count:
        issues.append(
            "post_evolution_count != mapped + failed + post duplicates"
        )
    # 演化后 delete 不校验字段，但同样计入 mapped_count；重复序 run 全部跳过。
    if applied not in (phase_pre + mapped_count, 0):
        issues.append(f"applied {applied} != pre applied {phase_pre} + mapped {mapped_count}")
    if applied + quarantined + skipped + marker_count != total_rows:
        issues.append(
            "applied + quarantined + skipped + marker != ledger rows"
        )
    if quarantined != quarantine_counts["gap"] + quarantine_counts["mapping"]:
        issues.append("quarantined_count != gap + mapping quarantine rows")
    expected_mapping_quarantine = (
        mapping_failed_count if mapping_policy == evolve.MAPPING_FAILURE_QUARANTINE else 0
    )
    if quarantine_counts["mapping"] != expected_mapping_quarantine:
        issues.append(
            f"mapping quarantine rows {quarantine_counts['mapping']} != expected {expected_mapping_quarantine}"
        )
    expected_mapping_skipped = (
        mapping_failed_count if mapping_policy == evolve.MAPPING_FAILURE_SKIP else 0
    )
    if skipped < expected_mapping_skipped:
        issues.append(
            f"skipped {skipped} < expected mapping skipped {expected_mapping_skipped}"
        )
    return issues

