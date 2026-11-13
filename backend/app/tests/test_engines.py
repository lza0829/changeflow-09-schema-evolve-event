from app.engines.apply_rules import classify_event, next_expected_seq
from app.modules import schema_evolve_event as evolve
from app.modules.schema_evolve_event import detect_evolution, normalize_payload, point_for_seq


def test_next_expected():
    assert next_expected_seq(0) == 1
    assert next_expected_seq(4) == 5


def test_classify_apply_and_gap():
    event = {"seq": 1, "op": "upsert", "entity_key": "A", "event_ts": "t", "payload": {}}
    assert classify_event(event=event, cursor=0, already_applied=False) == "apply"
    gap = {"seq": 3, "op": "upsert", "entity_key": "A", "event_ts": "t", "payload": {}}
    assert classify_event(event=gap, cursor=1, already_applied=False) == "quarantine_gap"
    assert (
        classify_event(event=event, cursor=0, already_applied=True) == "skipped_duplicate"
    )


def _upsert(seq, payload, **extra):
    return {
        "seq": seq,
        "op": "upsert",
        "entity_key": f"E{seq}",
        "event_ts": "2026-11-02T08:00:00Z",
        "payload": payload,
        **extra,
    }


def test_detect_evolve_event_and_declared_version():
    events = [
        _upsert(1, {"a": 1}),
        {
            "seq": 2,
            "op": "evolve",
            "entity_key": "__schema__",
            "event_ts": "t",
            "schema_version": 2,
            "from_version": 1,
            "payload": {},
        },
        _upsert(3, {"b": 2}),
    ]
    detected = detect_evolution(events, base_version=1)
    assert detected["final_version"] == 2
    assert detected["points"][0]["seq"] == 2
    assert detected["points"][0]["source"] == evolve.SOURCE_EVOLVE_EVENT

    # 无专用 evolve 事件时，普通事件显式 schema_version 也是隐式演化点。
    declared = detect_evolution([_upsert(1, {"a": 1}), _upsert(2, {"b": 2}, schema_version=2)])
    assert declared["points"][0]["seq"] == 2
    assert declared["points"][0]["source"] == evolve.SOURCE_DECLARED_VERSION


def test_point_for_seq_marks_pre_and_post():
    points = [{"seq": 3, "from_version": 1, "to_version": 2}]
    assert point_for_seq(points, 2) is None
    assert point_for_seq(points, 3)["to_version"] == 2
    assert point_for_seq(points, 4)["to_version"] == 2


def test_normalize_renames_legacy_fields():
    rule = {
        "from_version": 1,
        "to_version": 2,
        "fields": [
            {"from": "order_status", "to": "status", "required": True},
            {"from": "total_amount", "to": "amount", "required": True},
        ],
    }
    result = normalize_payload({"order_status": "paid", "total_amount": 10}, rule)
    assert result["ok"] is True
    assert result["payload"] == {"status": "paid", "amount": 10}
    assert set(result["renamed_fields"]) == {"order_status→status", "total_amount→amount"}


def test_normalize_accepts_new_shape_and_flags_missing():
    rule = {
        "fields": [
            {"from": "order_status", "to": "status", "required": True},
            {"from": "total_amount", "to": "amount", "required": True},
        ]
    }
    # 已是新契约：直接通过
    assert normalize_payload({"status": "paid", "amount": 10}, rule)["ok"] is True
    # 关键字段缺失：不得静默通过
    failed = normalize_payload({"status": "paid"}, rule)
    assert failed["ok"] is False
    assert failed["missing_fields"] == ["amount"]
    assert failed["reason"] == "schema_mapping_missing"


def test_normalize_without_rule_fails_closed():
    result = normalize_payload({"status": "paid"}, None)
    assert result["ok"] is False
    assert result["reason"] == "schema_mapping_unconfigured"


def test_validate_rule_fields_rejects_duplicates():
    import pytest

    with pytest.raises(ValueError):
        evolve.validate_rule_fields([])
    with pytest.raises(ValueError):
        evolve.validate_rule_fields(
            [
                {"from": "a", "to": "x"},
                {"from": "b", "to": "x"},
            ]
        )
    assert evolve.validate_rule_fields([{"from": "a", "to": "b"}]) == [
        {"from": "a", "to": "b", "required": False}
    ]
