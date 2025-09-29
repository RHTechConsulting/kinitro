"""
Event models for real-time broadcasting in Kinitro Backend.

This module defines Pydantic models for all event types that can be
broadcast to frontend clients via WebSocket connections.
"""

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field, field_serializer


class BaseEvent(BaseModel):
    """Base class for all event data models."""

    model_config = ConfigDict(
        # Validate assignment to ensure type safety
        validate_assignment=True,
        # Use Pydantic's JSON mode for proper serialization
        json_schema_serialization_defaults_required=True,
    )

    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    @field_serializer("timestamp", when_used="json")
    def serialize_timestamp(self, timestamp: datetime) -> str:
        """Serialize timestamp to ISO format string."""
        return timestamp.isoformat()

    @field_serializer("*", mode="wrap")
    def serialize_datetime_fields(self, value, serializer, info):
        """Automatically serialize any datetime field to ISO format."""
        if isinstance(value, datetime):
            return value.isoformat()
        return serializer(value)


# Job Events
class JobCreatedEvent(BaseEvent):
    """Event data for JOB_CREATED event."""

    job_id: str
    validator_hotkey: str
    competition_id: str
    submission_id: int
    miner_hotkey: str
    hf_repo_id: str
    env_provider: str
    benchmark_name: str
    config: Dict[str, Any]


class JobStatusChangedEvent(BaseEvent):
    """Event data for JOB_STATUS_CHANGED event."""

    job_id: str
    validator_hotkey: str
    status: str
    detail: Optional[str] = None
    created_at: datetime


class JobCompletedEvent(BaseEvent):
    """Event data for JOB_COMPLETED event."""

    job_id: str
    validator_hotkey: str
    status: str
    detail: Optional[str] = None
    result_count: int = 0


# Evaluation Events
class EvaluationStartedEvent(BaseEvent):
    """Event data for EVALUATION_STARTED event."""

    job_id: str
    validator_hotkey: str
    miner_hotkey: str
    competition_id: str
    submission_id: int
    benchmark_name: str


class EvaluationProgressEvent(BaseEvent):
    """Event data for EVALUATION_PROGRESS event."""

    job_id: str
    validator_hotkey: str
    miner_hotkey: str
    competition_id: str
    episodes_completed: int
    total_episodes: int
    current_score: float
    success_rate: float


class EvaluationCompletedEvent(BaseEvent):
    """Event data for EVALUATION_COMPLETED event."""

    job_id: str
    validator_hotkey: str
    miner_hotkey: str
    competition_id: str
    benchmark_name: str
    score: float
    success_rate: Optional[float] = None
    avg_reward: Optional[float] = None
    total_episodes: Optional[int] = None
    result_time: datetime
    created_at: datetime


# Episode Events
class EpisodeStartedEvent(BaseEvent):
    """Event data for EPISODE_STARTED event."""

    job_id: str
    submission_id: str
    episode_id: int
    env_name: str
    benchmark_name: str


class EpisodeStepEvent(BaseEvent):
    """Event data for EPISODE_STEP event."""

    submission_id: str
    episode_id: int
    step: int
    action: Dict[str, Any]
    reward: float
    done: bool
    truncated: bool
    observation_refs: Dict[str, Any]
    info: Optional[Dict[str, Any]] = None


class EpisodeCompletedEvent(BaseEvent):
    """Event data for EPISODE_COMPLETED event."""

    job_id: str
    submission_id: str
    episode_id: int
    env_name: str
    benchmark_name: str
    total_reward: float
    success: bool
    steps: int
    start_time: datetime
    end_time: datetime
    extra_metrics: Optional[Dict[str, Any]] = None
    created_at: datetime


# Competition Events
class CompetitionCreatedEvent(BaseEvent):
    """Event data for COMPETITION_CREATED event."""

    id: str
    name: str
    description: str
    benchmarks: List[str]
    points: int
    active: bool
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    created_at: datetime
    updated_at: datetime


class CompetitionUpdatedEvent(BaseEvent):
    """Event data for COMPETITION_UPDATED event."""

    id: str
    name: str
    description: str
    benchmarks: List[str]
    points: int
    active: bool
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    changes: Dict[str, Any]


class CompetitionActivatedEvent(BaseEvent):
    """Event data for COMPETITION_ACTIVATED event."""

    id: str
    name: str
    points: int


class CompetitionDeactivatedEvent(BaseEvent):
    """Event data for COMPETITION_DEACTIVATED event."""

    id: str
    name: str
    reason: Optional[str] = None


# Submission Events
class SubmissionReceivedEvent(BaseEvent):
    """Event data for SUBMISSION_RECEIVED event."""

    submission_id: int
    competition_id: str
    miner_hotkey: str
    hf_repo_id: str
    block_number: int
    created_at: datetime


# Validator Events
class ValidatorConnectedEvent(BaseEvent):
    """Event data for VALIDATOR_CONNECTED event."""

    validator_hotkey: str
    connection_id: str
    connected_at: datetime


class ValidatorDisconnectedEvent(BaseEvent):
    """Event data for VALIDATOR_DISCONNECTED event."""

    validator_hotkey: str
    connection_id: str
    disconnected_at: datetime
    reason: Optional[str] = None


# Stats Events
class StatsUpdatedEvent(BaseEvent):
    """Event data for STATS_UPDATED event."""

    total_competitions: int
    active_competitions: int
    total_points: int
    connected_validators: int
    total_submissions: int
    total_jobs: int
    total_results: int
    last_seen_block: int
    competition_percentages: Dict[str, float]
