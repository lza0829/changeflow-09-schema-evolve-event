"""流内 Schema 演化：识别演化点，并按旧→新字段映射规范化事件。

模型约定（与 README 中的说明保持一致）：

* 流有一个 ``base_schema_version``（默认 1）。
* 演化点有两种来源：
  - 专用 evolve 事件：``op == "evolve"``，target 版本取自顶层 ``schema_version``
    或 ``payload.to_version``；旧版本取自顶层 ``from_version`` 或
    ``payload.from_version``，缺省为“当前版本”。
  - 普通事件显式声明 ``schema_version``，且高于当前版本：该事件的 seq 视为
    隐式演化点（source=``event_schema_version``）。
* 演化点之前的事件属于“演化前”，按旧形状原样应用，Sink 行标注旧版本
  （``pre_evolution_policy = apply_legacy_shape``）。
* 演化点之后（含演化点 seq 上声明的普通事件）的事件属于“演化后”，必须按
  ``(from_version, to_version)`` 的映射规则规范化。已经使用新字段名的事件
  （目标字段已存在）直接通过；映射失败（必填目标字段缺失或规则未配置）时由
  调用方隔离或跳过，绝不静默写 Sink。
"""

from __future__ import annotations

from typing import Any

EVOLVE_OP = "evolve"

SOURCE_EVOLVE_EVENT = "evolve_event"
SOURCE_DECLARED_VERSION = "event_schema_version"

MAPPING_FAILURE_QUARANTINE = "quarantine"
MAPPING_FAILURE_SKIP = "skip"
MAPPING_FAILURE_POLICIES = (MAPPING_FAILURE_QUARANTINE, MAPPING_FAILURE_SKIP)

COMPAT_POLICY_PRE_EVOLUTION = "apply_legacy_shape"


def _as_int(value: Any, *, field: str, seq: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"invalid {field} at seq {seq}: {value!r}") from exc
    if parsed < 1:
        raise ValueError(f"invalid {field} at seq {seq}: {parsed}")
    return parsed


def _declared_version(event: dict[str, Any]) -> int | None:
    raw = event.get("schema_version")
    if raw is None:
        return None
    return _as_int(raw, field="schema_version", seq=int(event["seq"]))


def _evolve_target(event: dict[str, Any]) -> int:
    seq = int(event["seq"])
    target = event.get("schema_version")
    if target is None:
        payload = event.get("payload") or {}
        target = payload.get("to_version")
    if target is None:
        raise ValueError(
            f"evolve event at seq {seq} must declare schema_version or payload.to_version"
        )
    return _as_int(target, field="evolve target version", seq=seq)


def _evolve_from(event: dict[str, Any], current: int) -> int:
    seq = int(event["seq"])
    source = event.get("from_version")
    if source is None:
        payload = event.get("payload") or {}
        source = payload.get("from_version")
    if source is None:
        return current
    return _as_int(source, field="from_version", seq=seq)


def detect_evolution(
    events: list[dict[str, Any]], base_version: int = 1
) -> dict[str, Any]:
    """扫描事件序列，返回演化点与最终版本。

    结果结构::

        {
          "base_version": 1,
          "final_version": 2,
          "points": [
            {"seq": 3, "from_version": 1, "to_version": 2,
             "source": "evolve_event", "note": "..."}
          ]
        }
    """
    current = _as_int(base_version, field="base_version", seq=0)
    points: list[dict[str, Any]] = []
    for event in sorted(events, key=lambda e: int(e["seq"])):
        seq = int(event["seq"])
        if event.get("op") == EVOLVE_OP:
            # 幂等 evolve（重复投递 / 无版本变化的控制事件）也登记，但契约不变。
            target = _evolve_target(event)
            source_version = _evolve_from(event, current)
            points.append(
                {
                    "seq": seq,
                    "from_version": source_version,
                    "to_version": target,
                    "source": SOURCE_EVOLVE_EVENT,
                    "note": (event.get("payload") or {}).get("note"),
                }
            )
            current = target
            continue

        declared = _declared_version(event)
        if declared is not None and declared > current:
            points.append(
                {
                    "seq": seq,
                    "from_version": current,
                    "to_version": declared,
                    "source": SOURCE_DECLARED_VERSION,
                    "note": None,
                }
            )
            current = declared
        # declared < current 的事件视为旧版本事件；规范化仍以最近的演化点为准。
    return {"base_version": _as_int(base_version, field="base_version", seq=0),
            "final_version": current, "points": points}


def point_for_seq(points: list[dict[str, Any]], seq: int) -> dict[str, Any] | None:
    """返回 seq 处生效的演化点；None 表示仍属于演化前。"""
    applicable: dict[str, Any] | None = None
    for point in points:
        if int(point["seq"]) <= int(seq):
            applicable = point
        else:
            break
    return applicable


def normalize_payload(
    payload: dict[str, Any] | None, rule: dict[str, Any] | None
) -> dict[str, Any]:
    """按一条旧→新映射规则规范化 payload。

    规则形如::

        {"from_version": 1, "to_version": 2,
         "fields": [{"from": "order_status", "to": "status", "required": true}]}

    返回::

        {"ok": bool, "payload": normalised, "missing_fields": [...],
         "renamed_fields": ["order_status→status"], "reason": None|str}

    - 目标字段已存在：视为已是新契约，直接通过（可增量重放）。
    - 源字段存在、目标字段缺失：改名（从 payload 中移除旧字段）。
    - 源字段与目标字段都缺失且 required：记入 missing_fields，规范化失败。
    - 规则未配置（rule 为 None）：规范化失败，reason=``schema_mapping_unconfigured``。
    """
    data = dict(payload or {})
    if rule is None:
        return {
            "ok": False,
            "payload": data,
            "missing_fields": [],
            "renamed_fields": [],
            "reason": "schema_mapping_unconfigured",
        }

    missing: list[str] = []
    renamed: list[str] = []
    for field in rule.get("fields") or []:
        source = str(field.get("from") or "")
        target = str(field.get("to") or "")
        if not source or not target:
            continue
        if target in data:
            continue
        if source in data:
            data[target] = data.pop(source)
            renamed.append(f"{source}→{target}")
        elif bool(field.get("required")):
            missing.append(target)

    if missing:
        return {
            "ok": False,
            "payload": data,
            "missing_fields": missing,
            "renamed_fields": renamed,
            "reason": "schema_mapping_missing",
        }
    return {
        "ok": True,
        "payload": data,
        "missing_fields": [],
        "renamed_fields": renamed,
        "reason": None,
    }


def validate_rule_fields(raw_fields: Any) -> list[dict[str, Any]]:
    """校验并规范化规则字段列表（供 API 保存时使用）。"""
    if not isinstance(raw_fields, list) or not raw_fields:
        raise ValueError("fields must be a non-empty list")
    fields: list[dict[str, Any]] = []
    seen_targets: set[str] = set()
    for index, item in enumerate(raw_fields):
        if not isinstance(item, dict):
            raise ValueError(f"fields[{index}] must be an object")
        source = str(item.get("from") or "").strip()
        target = str(item.get("to") or "").strip()
        if not source or not target:
            raise ValueError(f"fields[{index}] requires non-empty from/to")
        if target in seen_targets:
            raise ValueError(f"duplicate target field: {target}")
        seen_targets.add(target)
        fields.append({"from": source, "to": target, "required": bool(item.get("required"))})
    return fields
