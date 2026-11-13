from fastapi import APIRouter, HTTPException

from app.config import FIXTURES_DIR
from app.db import connect
from app.engines.parse_change import parse_change_fixture
from app.modules import schema_evolve_event as evolve
from app.repositories import schema_registry
from app.repositories import streams as streams_repo
from app.schemas.entities import EvolutionPointPut, MappingPut

router = APIRouter(tags=["schema"])


def _stream_or_404(conn, stream_id: int) -> dict:
    stream = streams_repo.get_stream(conn, stream_id)
    if not stream:
        raise HTTPException(404, "stream not found")
    return stream


@router.get("/streams/{stream_id}/schema")
def get_schema(stream_id: int):
    """当前流登记的演化点 + 映射规则 + 夹具检测结果（刷新后仍在）。"""
    conn = connect()
    try:
        stream = _stream_or_404(conn, stream_id)
        base = int(stream.get("base_schema_version") or 1)
        detected = {"base_version": base, "final_version": base, "points": []}
        try:
            events = parse_change_fixture(FIXTURES_DIR / stream["fixture_path"])
            detected = evolve.detect_evolution(events, base_version=base)
        except Exception:  # noqa: BLE001 - 夹具缺失时仍可维护配置
            pass
        return {
            "stream": stream,
            "base_schema_version": base,
            "mapping_failure_policy": stream.get("mapping_failure_policy") or "quarantine",
            "points": schema_registry.list_points(conn, stream_id),
            "mappings": schema_registry.list_mappings(conn, stream_id),
            "detected": detected,
        }
    finally:
        conn.close()


@router.put("/streams/{stream_id}/schema/mappings")
def put_mapping(stream_id: int, body: MappingPut):
    conn = connect()
    try:
        _stream_or_404(conn, stream_id)
        try:
            mapping = schema_registry.upsert_mapping(
                conn,
                stream_id=stream_id,
                from_version=body.from_version,
                to_version=body.to_version,
                fields=[f.model_dump(by_alias=True) for f in body.fields],
                note=body.note,
            )
        except ValueError as exc:
            raise HTTPException(400, str(exc)) from exc
        conn.commit()
        return mapping
    finally:
        conn.close()


@router.delete("/streams/{stream_id}/schema/mappings/{mapping_id}")
def remove_mapping(stream_id: int, mapping_id: int):
    conn = connect()
    try:
        if not schema_registry.delete_mapping(conn, stream_id, mapping_id):
            raise HTTPException(404, "mapping not found")
        conn.commit()
        return {"deleted": mapping_id}
    finally:
        conn.close()


@router.put("/streams/{stream_id}/schema/points")
def put_point(stream_id: int, body: EvolutionPointPut):
    conn = connect()
    try:
        _stream_or_404(conn, stream_id)
        try:
            point = schema_registry.register_point(
                conn,
                stream_id=stream_id,
                seq=body.seq,
                from_version=body.from_version,
                to_version=body.to_version,
                source="manual",
                note=body.note,
            )
        except ValueError as exc:
            raise HTTPException(400, str(exc)) from exc
        conn.commit()
        return point
    finally:
        conn.close()


@router.delete("/streams/{stream_id}/schema/points/{point_id}")
def remove_point(stream_id: int, point_id: int):
    conn = connect()
    try:
        if not schema_registry.delete_point(conn, stream_id, point_id):
            raise HTTPException(404, "point not found")
        conn.commit()
        return {"deleted": point_id}
    finally:
        conn.close()
