"""Job distribution models and utilities for parent-child validator communication."""

from datetime import datetime, timezone
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field

from core.schemas import ChainCommitmentResponse


class MessageType(str, Enum):
    """Message types for validator communication."""

    JOB_ASSIGNMENT = "job_assignment"
    HEARTBEAT = "heartbeat"
    STATUS_UPDATE = "status_update"
    CHILD_REGISTER = "child_register"
    CHILD_DISCONNECT = "child_disconnect"


class ValidatorMessage(BaseModel):
    """Base message for validator communication."""

    message_type: MessageType
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    sender_id: str
    message_id: str


class JobAssignmentMessage(ValidatorMessage):
    """Message for assigning jobs from parent to child validators."""

    message_type: MessageType = MessageType.JOB_ASSIGNMENT
    commitment: ChainCommitmentResponse
    job_priority: int = Field(default=1, ge=1, le=10)
    timeout_seconds: Optional[int] = Field(default=None, ge=1)


class HeartbeatMessage(ValidatorMessage):
    """Heartbeat message to maintain connection."""

    message_type: MessageType = MessageType.HEARTBEAT
    status: str = "active"
    queue_size: Optional[int] = None


class StatusUpdateMessage(ValidatorMessage):
    """Status update message from child to parent."""

    message_type: MessageType = MessageType.STATUS_UPDATE
    job_id: Optional[str] = None
    status: str
    details: Optional[str] = None


class ChildRegisterMessage(ValidatorMessage):
    """Message for child validator registration."""

    message_type: MessageType = MessageType.CHILD_REGISTER
    validator_hotkey: str
    capabilities: dict[str, str] = Field(default_factory=dict)


class ChildDisconnectMessage(ValidatorMessage):
    """Message for child validator disconnection."""

    message_type: MessageType = MessageType.CHILD_DISCONNECT
    reason: Optional[str] = None


class ValidatorConnectionInfo(BaseModel):
    """Information about a connected child validator."""

    validator_id: str
    hotkey: str
    connected_at: datetime
    last_heartbeat: datetime
    status: str = "active"
    queue_size: int = 0
    capabilities: dict[str, str] = Field(default_factory=dict)
