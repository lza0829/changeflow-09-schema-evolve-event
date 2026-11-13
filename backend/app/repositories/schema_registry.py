from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from typing import Any

from app.modules.schema_evolve_event import validate_rule_fields


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def list_mappings(conn: sqlite3.Connection, stream_id: int) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT id, stream_id, from_version, to_version, fields_json, note, created_at, updated_at
        FROM schema_mappings WHERE stream_id=? ORDER BY from_version, to_version
        """,
        (stream_id,),
    ).fetchall()
    out = []
    for r in rows:
        item = dict(r)
        item["fields"] = json.loads(item.pop("fields_json") or "[]")
        out.append(item)
    return out


def get_mapping(
    conn: sqlite3.Connection, stream_id: int, from_version: int, to_version: int
) -> dict[str, Any] | None:
    row = conn.execute(
        """
        SELECT id, stream_id, from_version, to_version, fields_json, note, created_at, updated_at
        FROM schema_mappings WHERE stream_id=? AND from_version=? AND to_version=?
        """,
        (stream_id, from_version, to_version),
    ).fetchone()
    if not row:
        return None
    item = dict(row)
    item["fields"] = json.loads(item.pop("fields_json") or "[]")
    return item


def upsert_mapping(
    conn: sqlite3.Connection,
    *,
    stream_id: int,
    from_version: int,
    to_version: int,
    fields: Any,
    note: str | None = None,
) -> dict[str, Any]:
    validated = validate_rule_fields(fields)
    if int(from_version) >= int(to_version):
        raise ValueError("from_version must be lower than to_version")
    conn.execute(
        """
        INSERT INTO schema_mappings(stream_id, from_version, to_version, fields_json, note, created_at, updated_at)
        VALUES (?,?,?,?,?,?,?)
        ON CONFLICT(stream_id, from_version, to_version) DO UPDATE SET
            fields_json=excluded.fields_json,
            note=excluded.note,
            updated_at=excluded.updated_at
        """,
        (
            stream_id,
            int(from_version),
            int(to_version),
            json.dumps(validated, ensure_ascii=False),
            note,
            _now(),
            _now(),
        ),
    )
    mapping = get_mapping(conn, stream_id, int(from_version), int(to_version))
    assert mapping is not None
    return mapping


def delete_mapping(conn: sqlite3.Connection, stream_id: int, mapping_id: int) -> bool:
    cur = conn.execute(
        "DELETE FROM schema_mappings WHERE id=? AND stream_id=?", (mapping_id, stream_id)
    )
    return cur.rowcount > 0


def list_points(conn: sqlite3.Connection, stream_id: int) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT id, stream_id, seq, from_version, to_version, source, note, created_at
        FROM schema_evolutions WHERE stream_id=? ORDER BY seq
        """,
        (stream_id,),
    ).fetchall()
    return [dict(r) for r in rows]


def register_point(
    conn: sqlite3.Connection,
    *,
    stream_id: int,
    seq: int,
    from_version: int,
    to_version: int,
    source: str = "manual",
    note: str | None = None,
) -> dict[str, Any]:
    if int(to_version) <= int(from_version):
        raise ValueError("to_version must be greater than from_version")
    if int(seq) < 1:
        raise ValueError("seq must be >= 1")
    conn.execute(
        """
        INSERT INTO schema_evolutions(stream_id, seq, from_version, to_version, source, note, created_at)
        VALUES (?,?,?,?,?,?,?)
        ON CONFLICT(stream_id, seq) DO UPDATE SET
            from_version=excluded.from_version,
            to_version=excluded.to_version,
            source=excluded.source,
            note=excluded.note
        """,
        (stream_id, int(seq), int(from_version), int(to_version), source, note, _now()),
    )
    row = conn.execute(
        """
        SELECT id, stream_id, seq, from_version, to_version, source, note, created_at
        FROM schema_evolutions WHERE stream_id=? AND seq=?
        """,
        (stream_id, int(seq)),
    ).fetchone()
    return dict(row) if row else {}


def delete_point(conn: sqlite3.Connection, stream_id: int, point_id: int) -> bool:
    cur = conn.execute(
        "DELETE FROM schema_evolutions WHERE id=? AND stream_id=?", (point_id, stream_id)
    )
    return cur.rowcount > 0


def persist_detected_points(
    conn: sqlite3.Connection, stream_id: int, points: list[dict[str, Any]]
) -> None:
    """把夹具扫描到的演化点落库，保证刷新后仍可查看；手工登记点优先。"""
    # 夹具已不再包含的非手工点清掉，避免页面展示与 Apply 实际使用不一致。
    detected_seqs = [int(p["seq"]) for p in points]
    if detected_seqs:
        placeholders = ",".join("?" for _ in detected_seqs)
        conn.execute(
            f"""
            DELETE FROM schema_evolutions
            WHERE stream_id=? AND source != 'manual' AND seq NOT IN ({placeholders})
            """,
            (stream_id, *detected_seqs),
        )
    else:
        conn.execute(
            "DELETE FROM schema_evolutions WHERE stream_id=? AND source != 'manual'",
            (stream_id,),
        )
    for point in points:
        conn.execute(
            """
            INSERT INTO schema_evolutions(stream_id, seq, from_version, to_version, source, note, created_at)
            VALUES (?,?,?,?,?,?,?)
            ON CONFLICT(stream_id, seq) DO UPDATE SET
                from_version=excluded.from_version,
                to_version=excluded.to_version,
                source=excluded.source,
                note=excluded.note
            WHERE schema_evolutions.source != 'manual'
            """,
            (
                stream_id,
                int(point["seq"]),
                int(point["from_version"]),
                int(point["to_version"]),
                str(point.get("source") or "detected"),
                point.get("note"),
                _now(),
            ),
        )
