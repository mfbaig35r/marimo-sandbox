"""Domain models for marimo-sandbox."""
import json
from enum import StrEnum

from pydantic import BaseModel, model_validator


class RunStatus(StrEnum):
    PENDING = "pending"
    SUCCESS = "success"
    ERROR = "error"


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

    @model_validator(mode="before")
    @classmethod
    def _parse_packages(cls, data: dict) -> dict:
        if isinstance(data.get("packages"), str):
            data = dict(data)
            data["packages"] = json.loads(data["packages"] or "[]")
        return data


class DeletedRunInfo(BaseModel):
    run_id: str
    notebook_path: str
