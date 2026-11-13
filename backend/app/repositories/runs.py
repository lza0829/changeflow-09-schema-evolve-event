from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from typing import Any


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def create_run(conn: sqlite3.Connection, stream_id: int, cursor_before: int) -> int:
    cur = conn.execute(
        """
        INSERT INTO apply_runs(
            stream_id, status, loaded_count, applied_count, quarantined_count, skipped_count,
            cursor_before, cursor_after, report_json, created_at,
            pre_evolution_count, post_evolution_count, mapped_count, mapping_failed_count,
            final_schema_version
        ) VALUES (?,?,0,0,0,0,?,?,?,?,0,0,0,0,1)
        """,
        (stream_id, "running", cursor_before, cursor_before, "{}", _now()),
    )
    return int(cur.lastrowid)


def finish_run(
    conn: sqlite3.Connection,
    run_id: int,
    *,
    status: str,
    loaded: int,
    applied: int,
    quarantined: int,
    skipped: int,
    cursor_after: int,
    report: dict[str, Any],
    schema: dict[str, Any] | None = None,
) -> None:
    schema = schema or {}
    conn.execute(
        """
        UPDATE apply_runs SET status=?, loaded_count=?, applied_count=?, quarantined_count=?,
            skipped_count=?, cursor_after=?, report_json=?,
            pre_evolution_count=?, post_evolution_count=?, mapped_count=?, mapping_failed_count=?,
            final_schema_version=?
        WHERE id=?
        """,
        (
            status,
            loaded,
            applied,
            quarantined,
            skipped,
            cursor_after,
            json.dumps(report, ensure_ascii=False),
            int(schema.get("pre_evolution_count", 0)),
            int(schema.get("post_evolution_count", 0)),
            int(schema.get("mapped_count", 0)),
            int(schema.get("mapping_failed_count", 0)),
            int(schema.get("final_schema_version", 1)),
            run_id,
        ),
    )


def list_runs(conn: sqlite3.Connection, limit: int = 50) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT id, stream_id, status, loaded_count, applied_count, quarantined_count, skipped_count,
               cursor_before, cursor_after, report_json, created_at,
               pre_evolution_count, post_evolution_count, mapped_count, mapping_failed_count,
               final_schema_version
        FROM apply_runs ORDER BY id DESC LIMIT ?
        """,
        (limit,),
    ).fetchall()
    out = []
    for r in rows:
        item = dict(r)
        item["report"] = json.loads(item.pop("report_json") or "{}")
        out.append(item)
    return out


def get_run(conn: sqlite3.Connection, run_id: int) -> dict[str, Any] | None:
    row = conn.execute(
        """
        SELECT id, stream_id, status, loaded_count, applied_count, quarantined_count, skipped_count,
               cursor_before, cursor_after, report_json, created_at,
               pre_evolution_count, post_evolution_count, mapped_count, mapping_failed_count,
               final_schema_version
        FROM apply_runs WHERE id=?
        """,
        (run_id,),
    ).fetchone()
    if not row:
        return None
    item = dict(row)
    item["report"] = json.loads(item.pop("report_json") or "{}")
    return item
