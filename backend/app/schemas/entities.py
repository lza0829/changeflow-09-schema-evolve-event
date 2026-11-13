from typing import Any

from pydantic import BaseModel, Field


class ApplyRequest(BaseModel):
    stream_id: int


class StreamPatch(BaseModel):
    enabled: bool | None = None
    mapping_failure_policy: str | None = None


class SettingPut(BaseModel):
    value: str


class MappingField(BaseModel):
    from_: str = Field(alias="from")
    to: str
    required: bool = False

    model_config = {"populate_by_name": True}


class MappingPut(BaseModel):
    from_version: int
    to_version: int
    fields: list[MappingField]
    note: str | None = None


class EvolutionPointPut(BaseModel):
    seq: int
    from_version: int
    to_version: int
    note: str | None = None
