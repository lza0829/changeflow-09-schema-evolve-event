from __future__ import annotations

import json
from pathlib import Path
from typing import Any

SUPPORTED_OPS = ("upsert", "delete", "evolve")
SCHEMA_MARKER_ENTITY = "__schema__"


def parse_change_fixture(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        raise FileNotFoundError(f"fixture missing: {path}")
    events: list[dict[str, Any]] = []
    for line_no, raw in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        line = raw.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError as exc:
            raise ValueError(f"invalid jsonl at line {line_no}: {exc}") from exc
        for key in ("seq", "op", "event_ts"):
            if key not in obj:
                raise ValueError(f"missing {key} at line {line_no}")
        op = str(obj["op"]).lower()
        if op not in SUPPORTED_OPS:
            raise ValueError(f"unsupported op at line {line_no}: {op}")
        if "entity_key" not in obj and op != "evolve":
            raise ValueError(f"missing entity_key at line {line_no}")
        event = {
            "seq": int(obj["seq"]),
            "op": op,
            "entity_key": str(obj.get("entity_key") or SCHEMA_MARKER_ENTITY),
            "event_ts": str(obj["event_ts"]),
            "payload": obj.get("payload") or {},
        }
        # 版本声明随事件透传，供演化点识别与规范化使用。
        if obj.get("schema_version") is not None:
            event["schema_version"] = obj["schema_version"]
        if obj.get("from_version") is not None:
            event["from_version"] = obj["from_version"]
        events.append(event)
    events.sort(key=lambda e: e["seq"])
    return events
