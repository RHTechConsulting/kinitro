import enum
from datetime import datetime

from pydantic import Field
from sqlalchemy import text
from sqlalchemy.sql import func
from sqlmodel import Field as SQLModelField
from sqlmodel import SQLModel
from typing_extensions import Annotated

SnowflakeId = Annotated[int, Field(ge=0, le=(2**63 - 1))]


class EvaluationStatus(enum.Enum):
    """Evaluation job status enum."""

    QUEUED = "QUEUED"
    STARTING = "STARTING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    TIMEOUT = "TIMEOUT"


class TimestampMixin(SQLModel):
    """Mixin for created/updated timestamps."""

    created_at: datetime = SQLModelField(
        default=None,
        nullable=False,
        sa_column_kwargs={"server_default": text("CURRENT_TIMESTAMP"), "index": True},
    )
    updated_at: datetime = SQLModelField(
        default=None,
        nullable=False,
        sa_column_kwargs={
            "server_default": text("CURRENT_TIMESTAMP"),
            "onupdate": func.now(),
        },
    )
