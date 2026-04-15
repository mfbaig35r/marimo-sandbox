"""Domain models for marimo-sandbox."""
import json
from enum import StrEnum

from pydantic import BaseModel, model_validator


class RunStatus(StrEnum):
    PENDING = "pending"
    SUCCESS = "success"
    ERROR = "error"


class ArtifactInfo(BaseModel):
    path: str           # relative to notebook dir
    size_bytes: int
    extension: str      # e.g. ".csv", ".png", "" for no extension


class RunRecord(BaseModel):
    run_id: str
    description: str
    code: str
    status: RunStatus
    notebook_path: str
    packages: list[str] = []
    duration_ms: int | None = None
    stdout: str | None = None
    stderr: str | None = None
    error: str | None = None
    created_at: str
    code_hash: str | None = None
    freeze: str | None = None       # full pip freeze text; only set when packages installed
    artifacts: list[str] = []       # relative paths of user-created files

    @model_validator(mode="before")
    @classmethod
    def _parse_json_lists(cls, data: dict) -> dict:
        data = dict(data)
        if isinstance(data.get("packages"), str):
            data["packages"] = json.loads(data["packages"] or "[]")
        if isinstance(data.get("artifacts"), str):
            data["artifacts"] = json.loads(data["artifacts"] or "[]")
        return data


class DeletedRunInfo(BaseModel):
    run_id: str
    notebook_path: str
