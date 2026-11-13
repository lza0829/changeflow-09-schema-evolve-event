from __future__ import annotations

import json
import sqlite3
from typing import Any


def insert_ledger(
    conn: sqlite3.Connection,
    *,
    stream_id: int,
    run_id: int,
    event: dict[str, Any],
    disposition: str,
    schema_version: int | None = None,
    evolution_phase: str | None = None,
    mapping: dict[str, Any] | None = None,
) -> None:
    conn.execute(
        """
        INSERT INTO change_ledger(
            stream_id, run_id, seq, op, entity_key, event_ts, payload_json, disposition,
            schema_version, evolution_phase, mapping_json
        )
        VALUES (?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            stream_id,
            run_id,
            int(event["seq"]),
            event["op"],
            event["entity_key"],
            event["event_ts"],
            json.dumps(event.get("payload") or {}, ensure_ascii=False),
            disposition,
            schema_version,
            evolution_phase,
            json.dumps(mapping, ensure_ascii=False) if mapping is not None else None,
        ),
    )


def list_ledger(conn: sqlite3.Connection, limit: int = 200) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT id, stream_id, run_id, seq, op, entity_key, event_ts, payload_json, disposition,
               schema_version, evolution_phase, mapping_json
        FROM change_ledger ORDER BY id DESC LIMIT ?
        """,
        (limit,),
    ).fetchall()
    out = []
    for r in rows:
        item = dict(r)
        item["payload"] = json.loads(item.pop("payload_json") or "{}")
        raw_mapping = item.pop("mapping_json")
        item["mapping"] = json.loads(raw_mapping) if raw_mapping else None
        out.append(item)
    return out
