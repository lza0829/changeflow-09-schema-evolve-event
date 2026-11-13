from __future__ import annotations

import sqlite3

from app.db import connect

SCHEMA = """
CREATE TABLE IF NOT EXISTS settings(
    key TEXT PRIMARY KEY,
    value TEXT
);
CREATE TABLE IF NOT EXISTS streams(
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    fixture_path TEXT NOT NULL,
    entity_type TEXT NOT NULL,
    apply_cursor INTEGER NOT NULL DEFAULT 0,
    enabled INTEGER NOT NULL DEFAULT 1,
    note TEXT
);
CREATE TABLE IF NOT EXISTS apply_runs(
    id INTEGER PRIMARY KEY,
    stream_id INTEGER NOT NULL,
    status TEXT NOT NULL,
    loaded_count INTEGER NOT NULL,
    applied_count INTEGER NOT NULL,
    quarantined_count INTEGER NOT NULL,
    skipped_count INTEGER NOT NULL,
    cursor_before INTEGER NOT NULL,
    cursor_after INTEGER NOT NULL,
    report_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS change_ledger(
    id INTEGER PRIMARY KEY,
    stream_id INTEGER NOT NULL,
    run_id INTEGER,
    seq INTEGER NOT NULL,
    op TEXT NOT NULL,
    entity_key TEXT NOT NULL,
    event_ts TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    disposition TEXT NOT NULL,
    UNIQUE(stream_id, seq, run_id)
);
CREATE TABLE IF NOT EXISTS sink_records(
    id INTEGER PRIMARY KEY,
    stream_id INTEGER NOT NULL,
    entity_key TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    last_seq INTEGER NOT NULL,
    last_event_ts TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(stream_id, entity_key)
);
CREATE TABLE IF NOT EXISTS quarantine(
    id INTEGER PRIMARY KEY,
    stream_id INTEGER NOT NULL,
    run_id INTEGER NOT NULL,
    seq INTEGER NOT NULL,
    entity_key TEXT NOT NULL,
    reason TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS applied_seqs(
    stream_id INTEGER NOT NULL,
    seq INTEGER NOT NULL,
    run_id INTEGER NOT NULL,
    applied_at TEXT NOT NULL,
    PRIMARY KEY(stream_id, seq)
);
CREATE TABLE IF NOT EXISTS schema_mappings(
    id INTEGER PRIMARY KEY,
    stream_id INTEGER NOT NULL,
    from_version INTEGER NOT NULL,
    to_version INTEGER NOT NULL,
    fields_json TEXT NOT NULL,
    note TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(stream_id, from_version, to_version)
);
CREATE TABLE IF NOT EXISTS schema_evolutions(
    id INTEGER PRIMARY KEY,
    stream_id INTEGER NOT NULL,
    seq INTEGER NOT NULL,
    from_version INTEGER NOT NULL,
    to_version INTEGER NOT NULL,
    source TEXT NOT NULL,
    note TEXT,
    created_at TEXT NOT NULL,
    UNIQUE(stream_id, seq)
);
"""

# 已有本地 SQLite 的轻量迁移（只加列，不删数据）。
MIGRATIONS: dict[str, list[tuple[str, str]]] = {
    "streams": [
        ("base_schema_version", "INTEGER NOT NULL DEFAULT 1"),
        ("mapping_failure_policy", "TEXT NOT NULL DEFAULT 'quarantine'"),
    ],
    "apply_runs": [
        ("pre_evolution_count", "INTEGER NOT NULL DEFAULT 0"),
        ("post_evolution_count", "INTEGER NOT NULL DEFAULT 0"),
        ("mapped_count", "INTEGER NOT NULL DEFAULT 0"),
        ("mapping_failed_count", "INTEGER NOT NULL DEFAULT 0"),
        ("final_schema_version", "INTEGER NOT NULL DEFAULT 1"),
    ],
    "change_ledger": [
        ("schema_version", "INTEGER"),
        ("evolution_phase", "TEXT"),
        ("mapping_json", "TEXT"),
    ],
    "sink_records": [
        ("schema_version", "INTEGER NOT NULL DEFAULT 1"),
    ],
}


def _migrate(conn: sqlite3.Connection) -> None:
    for table, columns in MIGRATIONS.items():
        existing = {r["name"] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()}
        for name, ddl in columns:
            if name not in existing:
                conn.execute(f"ALTER TABLE {table} ADD COLUMN {name} {ddl}")


def init_db():
    conn = connect()
    conn.executescript(SCHEMA)
    _migrate(conn)
    if conn.execute("SELECT COUNT(*) c FROM streams").fetchone()["c"] == 0:
        conn.executemany(
            """
            INSERT INTO streams(
                name, fixture_path, entity_type, apply_cursor, enabled, note,
                base_schema_version, mapping_failure_policy
            )
            VALUES (?,?,?,?,?,?,?,?)
            """,
            [
                (
                    "Orders CDC",
                    "orders_cdc.jsonl",
                    "order",
                    0,
                    1,
                    "clean：seq 1→5 连续；含一次 delete",
                    1,
                    "quarantine",
                ),
                (
                    "Inventory CDC",
                    "inventory_cdc.jsonl",
                    "inventory",
                    0,
                    1,
                    "clean：seq 1→3 连续库存变更",
                    1,
                    "quarantine",
                ),
                (
                    "Orders CDC · Schema Evolve",
                    "orders_schema_evolve.jsonl",
                    "order",
                    0,
                    1,
                    "v1→v2：seq 3 演化点；order_status/total_amount → status/amount",
                    1,
                    "quarantine",
                ),
            ],
        )
        conn.execute("INSERT INTO settings(key, value) VALUES ('project_label', 'Changeflow')")
        conn.execute(
            "INSERT INTO settings(key, value) VALUES ('gap_policy', 'stop_quarantine')"
        )
        conn.commit()
    _ensure_evolution_demo(conn)
    conn.close()


def _ensure_evolution_demo(conn: sqlite3.Connection) -> None:
    """老数据目录里也补齐演化演示流与其默认映射规则。"""
    row = conn.execute(
        "SELECT id FROM streams WHERE fixture_path='orders_schema_evolve.jsonl'"
    ).fetchone()
    if row is None:
        conn.execute(
            """
            INSERT INTO streams(
                name, fixture_path, entity_type, apply_cursor, enabled, note,
                base_schema_version, mapping_failure_policy
            ) VALUES (?,?,?,0,1,?,1,'quarantine')
            """,
            (
                "Orders CDC · Schema Evolve",
                "orders_schema_evolve.jsonl",
                "order",
                "v1→v2：seq 3 演化点；order_status/total_amount → status/amount",
            ),
        )
        row = conn.execute(
            "SELECT id FROM streams WHERE fixture_path='orders_schema_evolve.jsonl'"
        ).fetchone()
    stream_id = int(row["id"])
    existing = conn.execute(
        "SELECT COUNT(*) c FROM schema_mappings WHERE stream_id=?", (stream_id,)
    ).fetchone()["c"]
    if existing == 0:
        from app.repositories import schema_registry

        schema_registry.upsert_mapping(
            conn,
            stream_id=stream_id,
            from_version=1,
            to_version=2,
            fields=[
                {"from": "order_status", "to": "status", "required": True},
                {"from": "total_amount", "to": "amount", "required": True},
                {"from": "sku", "to": "sku", "required": False},
            ],
            note="v2 契约：状态/金额改名；sku 保持不变但登记为必需字段校验",
        )
    conn.commit()
