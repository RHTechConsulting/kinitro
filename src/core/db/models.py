import enum
from datetime import datetime

from pydantic import Field
from sqlalchemy import (
    DateTime,
    func,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Mapped, mapped_column
from typing_extensions import Annotated

SnowflakeId = Annotated[int, Field(ge=0, le=(2**63 - 1))]

# Base class for all validator models
Base = declarative_base()


class EvaluationStatus(enum.Enum):
    """Evaluation job status enum."""

    QUEUED = "QUEUED"
    STARTING = "STARTING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    TIMEOUT = "TIMEOUT"


class TimestampMixin:
    """Mixin for created/updated timestamps."""

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), index=True
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )
