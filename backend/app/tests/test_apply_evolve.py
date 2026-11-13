"""端到端：流内 Schema 演化 Apply，报告/账本/隔离/Sink 同源对账。"""

import json

from app.db import connect
from app.repositories import schema_registry
from app.seed import init_db
from app.services.apply_service import run_apply


def _stream_id(fixture_name: str) -> int:
    conn = connect()
    try:
        row = conn.execute(
            "SELECT id FROM streams WHERE fixture_path=?", (fixture_name,)
        ).fetchone()
        return int(row["id"])
    finally:
        conn.close()


def test_evolve_apply_end_to_end(db):
    stream_id = _stream_id("orders_schema_evolve.jsonl")
    run = run_apply(stream_id)
    evolution = run["report"]["evolution"]

    assert run["status"] == "succeeded"
    assert run["final_schema_version"] == 2
    assert evolution["consistent"] is True
    assert evolution["inconsistencies"] == []

    # 数字必须与账本/隔离列表互相解释
    assert evolution["pre_evolution_count"] == evolution["ledger_counts"]["pre_evolution"] == 3
    assert evolution["post_evolution_count"] == evolution["ledger_counts"]["post_evolution"] == 4
    assert evolution["marker_count"] == 1
    assert evolution["mapped_count"] + evolution["mapping_failed_count"] == 4
    assert evolution["mapped_count"] == 3
    assert evolution["mapping_failed_count"] == 1
    assert run["applied_count"] == 6  # 3 演化前 + 3 映射成功
    assert run["quarantined_count"] == 1
    assert evolution["quarantine_counts"]["mapping"] == 1
    assert evolution["sink_schema_versions"] == {"1": 1, "2": 2}

    conn = connect()
    try:
        sink = {
            r["entity_key"]: r
            for r in conn.execute(
                "SELECT entity_key, payload_json, schema_version FROM sink_records "
                "WHERE stream_id=?",
                (stream_id,),
            ).fetchall()
        }
        # 演化前事件保持旧形状（未被 v2 映射改写）
        legacy = sink["ORD-5003"]
        assert legacy["schema_version"] == 1
        legacy_payload = json.loads(legacy["payload_json"])
        assert "order_status" in legacy_payload
        assert "status" not in legacy_payload
        # 演化后事件按映射规范化到新字段
        post = sink["ORD-5001"]
        assert post["schema_version"] == 2
        post_payload = json.loads(post["payload_json"])
        assert "status" in post_payload
        assert "order_status" not in post_payload
        # 映射缺关键字段的事件不得进 Sink
        assert "ORD-5005" not in sink
        quarantined = conn.execute(
            "SELECT reason FROM quarantine WHERE stream_id=? AND seq=7", (stream_id,)
        ).fetchone()
        assert quarantined["reason"] == "schema_mapping_missing"
    finally:
        conn.close()


def test_mapping_policy_skip_does_not_quarantine(db):
    stream_id = _stream_id("orders_schema_evolve.jsonl")
    conn = connect()
    try:
        conn.execute(
            "UPDATE streams SET mapping_failure_policy='skip' WHERE id=?", (stream_id,)
        )
        conn.commit()
    finally:
        conn.close()
    run = run_apply(stream_id)
    evolution = run["report"]["evolution"]
    assert evolution["mapping_failure_policy"] == "skip"
    assert run["quarantined_count"] == 0
    assert run["skipped_count"] == 1
    assert evolution["consistent"] is True


def test_mapping_rules_persist_and_are_used(db):
    """保存的映射规则在刷新（新连接）后仍可读到，并进入报告。"""
    stream_id = _stream_id("orders_schema_evolve.jsonl")
    conn = connect()
    try:
        schema_registry.upsert_mapping(
            conn,
            stream_id=stream_id,
            from_version=1,
            to_version=2,
            fields=[
                {"from": "order_status", "to": "state", "required": True},
                {"from": "total_amount", "to": "amount", "required": True},
            ],
            note="自定义",
        )
        conn.commit()
        saved = schema_registry.get_mapping(conn, stream_id, 1, 2)
        target = saved["fields"][0]["to"]
    finally:
        conn.close()
    assert target == "state"
    run = run_apply(stream_id)
    rule = run["report"]["evolution"]["mapping_rules"][0]
    assert rule["configured"] is True
    assert rule["fields"][0]["to"] == "state"


def test_no_mapping_rule_quarantines_post_events(db):
    """未配置映射规则时，演化后事件不得静默写 Sink。"""
    stream_id = _stream_id("orders_schema_evolve.jsonl")
    conn = connect()
    try:
        for m in schema_registry.list_mappings(conn, stream_id):
            schema_registry.delete_mapping(conn, stream_id, m["id"])
        conn.commit()
    finally:
        conn.close()
    run = run_apply(stream_id)
    evolution = run["report"]["evolution"]
    assert evolution["mapping_rules"][0]["configured"] is False
    # delete 按 key 删除不校验字段，仍计为 mapped；其余 3 条 upsert 全部失败。
    assert evolution["mapped_count"] == 1
    assert evolution["mapping_failed_count"] == evolution["post_evolution_count"] - 1
    assert run["applied_count"] == evolution["pre_evolution_count"] + 1  # + 1 = delete
    conn = connect()
    try:
        reasons = {
            r["reason"]
            for r in conn.execute(
                "SELECT reason FROM quarantine WHERE stream_id=?", (stream_id,)
            ).fetchall()
        }
        assert "schema_mapping_unconfigured" in reasons
        # 演化后无映射成功事件写入新形状；Sink 只有旧形状行
        versions = {
            int(r["schema_version"])
            for r in conn.execute(
                "SELECT schema_version FROM sink_records WHERE stream_id=?",
                (stream_id,),
            ).fetchall()
        }
        assert versions == {1}
    finally:
        conn.close()


def test_manual_point_overrides_detected(db):
    """手工登记点与夹具检测点同 seq 冲突时，以手工为准。"""
    stream_id = _stream_id("orders_schema_evolve.jsonl")
    conn = connect()
    try:
        # 夹具检测到 seq 4: v1→v2；手工改成 seq 4: v1→v3
        schema_registry.register_point(
            conn,
            stream_id=stream_id,
            seq=4,
            from_version=1,
            to_version=3,
            source="manual",
            note="手工改版本",
        )
        schema_registry.upsert_mapping(
            conn,
            stream_id=stream_id,
            from_version=1,
            to_version=3,
            fields=[
                {"from": "order_status", "to": "state", "required": True},
                {"from": "total_amount", "to": "total", "required": True},
            ],
        )
        conn.commit()
    finally:
        conn.close()
    run = run_apply(stream_id)
    evolution = run["report"]["evolution"]
    assert evolution["final_schema_version"] == 3
    assert evolution["points"][0]["source"] == "manual"
    assert evolution["points"][0]["to_version"] == 3
    # v3 映射要求 state/total：旧形状 seq5 可映射；新形状 seq6 与缺字段 seq7 失败；delete 不校验
    assert evolution["mapped_count"] == 2  # seq5 + delete
    assert evolution["mapping_failed_count"] == 2  # seq6 + seq7
    assert evolution["post_evolution_count"] == 4
    assert evolution["consistent"] is True
