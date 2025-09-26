"""
Real-time update system for Kinitro Backend.

This module provides WebSocket-based real-time updates to frontend clients,
including evaluation results, job status updates, episode data streaming, etc.
"""

import asyncio
import json
import uuid
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from fastapi import WebSocket
from sqlalchemy import func, select

from core.log import get_logger
from core.messages import (
    EventMessage,
    EventType,
    MessageType,
    PongMessage,
    SubscribeMessage,
    SubscriptionAckMessage,
    SubscriptionRequest,
    UnsubscribeMessage,
    UnsubscriptionAckMessage,
)

if TYPE_CHECKING:
    from backend.service import BackendService

# Import models at runtime to avoid circular imports
try:
    from backend.models import (
        BackendEvaluationJob,
        BackendEvaluationJobStatus,
        BackendEvaluationResult,
        BackendState,
        Competition,
        MinerSubmission,
        ValidatorConnection,
    )
except ImportError:
    # Handle case where models might not be available during testing
    pass

logger = get_logger(__name__)


class ClientConnection:
    """Represents a connected client with their subscriptions."""

    def __init__(self, connection_id: str, websocket: WebSocket):
        self.connection_id = connection_id
        self.websocket = websocket
        self.subscriptions: Dict[str, SubscriptionRequest] = {}
        self.connected_at = datetime.now(timezone.utc)
        self.last_ping = datetime.now(timezone.utc)

    def add_subscription(self, subscription_id: str, request: SubscriptionRequest):
        """Add a subscription for this client."""
        self.subscriptions[subscription_id] = request

    def remove_subscription(self, subscription_id: str) -> bool:
        """Remove a subscription. Returns True if subscription existed."""
        if subscription_id in self.subscriptions:
            del self.subscriptions[subscription_id]
            return True
        return False

    def should_receive_event(
        self, event_type: EventType, event_data: Dict[str, Any]
    ) -> List[str]:
        """
        Check if this client should receive an event.
        Returns list of subscription IDs that match.
        """
        matching_subscriptions = []

        logger.debug(
            f"Client {self.connection_id} checking event {event_type} against {len(self.subscriptions)} subscriptions"
        )

        for sub_id, subscription in self.subscriptions.items():
            # Check if event type is subscribed
            if event_type not in subscription.event_types:
                logger.debug(
                    f"Subscription {sub_id} does not include event type {event_type}"
                )
                continue

            # Check filters
            matches = True
            for filter_key, filter_value in subscription.filters.items():
                if filter_key in event_data:
                    if event_data[filter_key] != filter_value:
                        matches = False
                        logger.debug(
                            f"Filter mismatch: {filter_key}={event_data[filter_key]} != {filter_value}"
                        )
                        break

            if matches:
                matching_subscriptions.append(sub_id)
                logger.debug(f"Subscription {sub_id} matches event {event_type}")

        logger.debug(
            f"Client {self.connection_id} has {len(matching_subscriptions)} matching subscriptions"
        )
        return matching_subscriptions


class RealtimeEventBroadcaster:
    """
    Central event broadcaster for real-time updates.
    Manages client connections and event distribution.
    """

    def __init__(self, backend_service: Optional["BackendService"] = None):
        self.client_connections: Dict[str, ClientConnection] = {}
        self._broadcast_queue: asyncio.Queue = asyncio.Queue()
        self._running = False
        self._broadcast_task = None
        self.backend_service = backend_service

    def set_backend_service(self, backend_service: "BackendService"):
        """Set the backend service reference for database access."""
        self.backend_service = backend_service

    async def start(self):
        """Start the event broadcaster."""
        self._running = True
        self._broadcast_task = asyncio.create_task(self._process_broadcast_queue())
        logger.info("Realtime event broadcaster started")

    async def stop(self):
        """Stop the event broadcaster."""
        self._running = False
        if self._broadcast_task:
            self._broadcast_task.cancel()
            try:
                await self._broadcast_task
            except asyncio.CancelledError:
                pass

        # Close all client connections
        for connection in self.client_connections.values():
            await connection.websocket.close()

        self.client_connections.clear()
        logger.info("Realtime event broadcaster stopped")

    async def add_client(
        self, connection_id: str, websocket: WebSocket
    ) -> ClientConnection:
        """Add a new client connection."""
        connection = ClientConnection(connection_id, websocket)
        self.client_connections[connection_id] = connection
        logger.info(f"Client {connection_id} connected")
        return connection

    async def remove_client(self, connection_id: str):
        """Remove a client connection."""
        if connection_id in self.client_connections:
            del self.client_connections[connection_id]
            logger.info(f"Client {connection_id} disconnected")

    async def broadcast_event(self, event_type: EventType, event_data: Dict[str, Any]):
        """
        Broadcast an event to all relevant clients.
        This is the main entry point for sending events.
        """
        logger.debug(
            f"Broadcasting event: {event_type} to {len(self.client_connections)} clients"
        )
        await self._broadcast_queue.put((event_type, event_data))

    async def _process_broadcast_queue(self):
        """Process events from the broadcast queue and send to clients."""
        while self._running:
            try:
                # Wait for events with timeout to allow checking _running flag
                event_type, event_data = await asyncio.wait_for(
                    self._broadcast_queue.get(), timeout=1.0
                )

                logger.debug(
                    f"Processing event: {event_type} for {len(self.client_connections)} clients"
                )

                # Send to all relevant clients
                disconnected_clients = []
                sent_count = 0

                for connection_id, connection in self.client_connections.items():
                    subscription_ids = connection.should_receive_event(
                        event_type, event_data
                    )

                    if subscription_ids:
                        # Client should receive this event
                        for sub_id in subscription_ids:
                            message = EventMessage(
                                event_type=event_type,
                                event_data=event_data,
                                subscription_id=sub_id,
                            )

                            try:
                                await connection.websocket.send_text(
                                    message.model_dump_json()
                                )
                                sent_count += 1
                                logger.debug(
                                    f"Sent {event_type} event to client {connection_id}"
                                )
                            except Exception as e:
                                logger.error(
                                    f"Failed to send event to client {connection_id}: {e}"
                                )
                                disconnected_clients.append(connection_id)

                # Remove disconnected clients
                for connection_id in disconnected_clients:
                    await self.remove_client(connection_id)

                if sent_count > 0:
                    logger.debug(
                        f"Successfully sent {event_type} event to {sent_count} clients"
                    )
                else:
                    logger.debug(f"No clients subscribed to {event_type} event")

            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logger.error(f"Error processing broadcast queue: {e}")

    async def handle_client_message(self, connection_id: str, message: str):
        """Handle a message from a client."""
        try:
            data = json.loads(message)
            message_type = data.get("message_type")

            connection = self.client_connections.get(connection_id)
            if not connection:
                logger.error(f"Unknown connection {connection_id}")
                return

            if message_type == MessageType.SUBSCRIBE:
                await self._handle_subscribe(connection, data)
            elif message_type == MessageType.UNSUBSCRIBE:
                await self._handle_unsubscribe(connection, data)
            elif message_type == MessageType.PING:
                await self._handle_ping(connection, data)
            else:
                error_msg = EventMessage(
                    event_type=EventType.STATS_UPDATED,  # Placeholder, should be error type
                    event_data={"error": f"Unknown message type: {message_type}"},
                )
                await connection.websocket.send_text(error_msg.model_dump_json())

        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON from client {connection_id}: {e}")
            connection = self.client_connections.get(connection_id)
            if connection:
                error_msg = EventMessage(
                    event_type=EventType.STATS_UPDATED,  # Placeholder
                    event_data={"error": "Invalid JSON format"},
                )
                await connection.websocket.send_text(error_msg.model_dump_json())
        except Exception as e:
            logger.error(f"Error handling client message: {e}")

    async def _handle_subscribe(self, connection: ClientConnection, data: dict):
        """Handle subscription request."""
        try:
            msg = SubscribeMessage.model_validate(data)

            # Generate subscription ID
            subscription_id = str(uuid.uuid4())

            # Add subscription
            connection.add_subscription(subscription_id, msg.subscription)

            # Send acknowledgment
            ack = SubscriptionAckMessage(
                subscription_id=subscription_id,
                subscribed_events=msg.subscription.event_types,
                request_id=msg.request_id,
            )
            await connection.websocket.send_text(ack.model_dump_json())

            # Send initial state data for relevant subscriptions
            await self._send_initial_state_data(
                connection, subscription_id, msg.subscription
            )

            logger.info(
                f"Client {connection.connection_id} subscribed to {msg.subscription.event_types} with subscription {subscription_id}"
            )
            logger.debug(
                f"Client {connection.connection_id} now has {len(connection.subscriptions)} subscriptions"
            )

        except Exception as e:
            logger.error(f"Failed to subscribe: {str(e)}")

    async def _handle_unsubscribe(self, connection: ClientConnection, data: dict):
        """Handle unsubscription request."""
        try:
            msg = UnsubscribeMessage.model_validate(data)

            # Remove subscription
            if connection.remove_subscription(msg.subscription_id):
                # Send acknowledgment
                ack = UnsubscriptionAckMessage(
                    subscription_id=msg.subscription_id, request_id=msg.request_id
                )
                await connection.websocket.send_text(ack.model_dump_json())

                logger.info(
                    f"Client {connection.connection_id} unsubscribed from {msg.subscription_id}"
                )
            else:
                logger.warning(
                    f"Subscription {msg.subscription_id} not found for client {connection.connection_id}"
                )

        except Exception as e:
            logger.error(f"Failed to unsubscribe: {str(e)}")

    async def _handle_ping(self, connection: ClientConnection, data: dict):
        """Handle ping message."""
        connection.last_ping = datetime.now(timezone.utc)
        pong = PongMessage(request_id=data.get("request_id"))
        await connection.websocket.send_text(pong.model_dump_json())

    async def _send_initial_state_data(
        self,
        connection: ClientConnection,
        subscription_id: str,
        subscription: SubscriptionRequest,
    ):
        """Send initial state data for applicable event types after subscription."""
        if not self.backend_service or not self.backend_service.async_session:
            logger.debug(
                "No backend service or database session available for initial state data"
            )
            return

        try:
            async with self.backend_service.async_session() as session:
                # Send initial data based on subscribed event types
                for event_type in subscription.event_types:
                    initial_data = await self._get_initial_state_for_event_type(
                        session, event_type, subscription.filters
                    )

                    if initial_data:
                        for data_item in initial_data:
                            message = EventMessage(
                                event_type=event_type,
                                event_data=data_item,
                                subscription_id=subscription_id,
                            )
                            await connection.websocket.send_text(
                                message.model_dump_json()
                            )
                            logger.debug(
                                f"Sent initial state data for {event_type} to client {connection.connection_id}"
                            )

        except Exception as e:
            logger.error(f"Failed to send initial state data: {str(e)}")

    async def _get_initial_state_for_event_type(
        self, session, event_type: EventType, filters: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Get initial state data for a specific event type."""
        try:
            if event_type == EventType.STATS_UPDATED:
                return await self._get_initial_stats_data(session)

            elif (
                event_type == EventType.JOB_STATUS_CHANGED
                or event_type == EventType.JOB_CREATED
            ):
                return await self._get_initial_job_data(session, filters)

            elif event_type == EventType.EVALUATION_COMPLETED:
                return await self._get_initial_evaluation_data(session, filters)

            elif (
                event_type == EventType.COMPETITION_ACTIVATED
                or event_type == EventType.COMPETITION_CREATED
            ):
                return await self._get_initial_competition_data(session, filters)

            # Add more event types as needed
            else:
                logger.debug(
                    f"No initial state data handler for event type: {event_type}"
                )
                return []

        except Exception as e:
            logger.error(f"Error getting initial state data for {event_type}: {str(e)}")
            return []

    async def _get_initial_stats_data(self, session) -> List[Dict[str, Any]]:
        """Get current backend statistics."""
        try:
            # Models are imported at module level

            # Get competitions
            comp_result = await session.execute(select(Competition))
            competitions = comp_result.scalars().all()
            active_comps = [c for c in competitions if c.active]
            total_points = sum(c.points for c in active_comps)

            # Get validators
            val_result = await session.execute(
                select(ValidatorConnection).where(ValidatorConnection.is_connected)
            )
            connected_validators = len(val_result.scalars().all())

            # Get submissions count
            sub_result = await session.execute(select(func.count(MinerSubmission.id)))
            total_submissions = sub_result.scalar() or 0

            # Get jobs count
            job_result = await session.execute(
                select(func.count(BackendEvaluationJob.id))
            )
            total_jobs = job_result.scalar() or 0

            # Get results count
            result_count = await session.execute(
                select(func.count(BackendEvaluationResult.id))
            )
            total_results = result_count.scalar() or 0

            # Get backend state
            state_result = await session.execute(
                select(BackendState).where(BackendState.id == 1)
            )
            state = state_result.scalar_one_or_none()

            # Calculate competition percentages
            comp_percentages = {}
            for comp in active_comps:
                percentage = (
                    (comp.points / total_points * 100) if total_points > 0 else 0
                )
                comp_percentages[comp.id] = percentage

            stats_data = {
                "total_competitions": len(competitions),
                "active_competitions": len(active_comps),
                "total_points": total_points,
                "connected_validators": connected_validators,
                "total_submissions": total_submissions,
                "total_jobs": total_jobs,
                "total_results": total_results,
                "last_seen_block": state.last_seen_block if state else 0,
                "competition_percentages": comp_percentages,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }

            return [stats_data]

        except Exception as e:
            logger.error(f"Error getting initial stats data: {str(e)}")
            return []

    async def _get_initial_validator_data(
        self, session, filters: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Get currently connected validators."""
        try:
            # ValidatorConnection is imported at module level

            query = select(ValidatorConnection).where(ValidatorConnection.is_connected)

            # Apply filters if any
            if filters:
                for filter_key, filter_value in filters.items():
                    if filter_key == "validator_hotkey":
                        query = query.where(
                            ValidatorConnection.validator_hotkey == filter_value
                        )

            result = await session.execute(query)
            validators = result.scalars().all()

            validator_data = []
            for validator in validators:
                data = {
                    "validator_hotkey": validator.validator_hotkey,
                    "connection_id": validator.connection_id,
                    "connected_at": validator.last_connected_at.isoformat()
                    if validator.last_connected_at
                    else None,
                    "is_connected": validator.is_connected,
                    "total_jobs_sent": validator.total_jobs_sent,
                    "total_results_received": validator.total_results_received,
                }
                validator_data.append(data)

            return validator_data

        except Exception as e:
            logger.error(f"Error getting initial validator data: {str(e)}")
            return []

    async def _get_initial_job_data(
        self, session, filters: Dict[str, Any], limit: int = 50
    ) -> List[Dict[str, Any]]:
        """Get recent job status data."""
        try:
            # BackendEvaluationJobStatus is imported at module level

            query = select(BackendEvaluationJobStatus)

            # Apply filters if any
            if filters:
                for filter_key, filter_value in filters.items():
                    if filter_key == "job_id":
                        query = query.where(
                            BackendEvaluationJobStatus.job_id == filter_value
                        )
                    elif filter_key == "validator_hotkey":
                        query = query.where(
                            BackendEvaluationJobStatus.validator_hotkey == filter_value
                        )

            query = query.order_by(BackendEvaluationJobStatus.created_at.desc()).limit(
                limit
            )

            result = await session.execute(query)
            job_statuses = result.scalars().all()

            job_data = []
            for job_status in job_statuses:
                data = {
                    "job_id": job_status.job_id,
                    "validator_hotkey": job_status.validator_hotkey,
                    "status": job_status.status.value,
                    "detail": job_status.detail,
                    "created_at": job_status.created_at.isoformat(),
                }
                job_data.append(data)

            return job_data

        except Exception as e:
            logger.error(f"Error getting initial job data: {str(e)}")
            return []

    async def _get_initial_evaluation_data(
        self, session, filters: Dict[str, Any], limit: int = 50
    ) -> List[Dict[str, Any]]:
        """Get recent evaluation results."""
        try:
            # BackendEvaluationResult is imported at module level

            query = select(BackendEvaluationResult)

            # Apply filters if any
            if filters:
                for filter_key, filter_value in filters.items():
                    if filter_key == "job_id":
                        query = query.where(
                            BackendEvaluationResult.job_id == filter_value
                        )
                    elif filter_key == "validator_hotkey":
                        query = query.where(
                            BackendEvaluationResult.validator_hotkey == filter_value
                        )
                    elif filter_key == "miner_hotkey":
                        query = query.where(
                            BackendEvaluationResult.miner_hotkey == filter_value
                        )
                    elif filter_key == "competition_id":
                        query = query.where(
                            BackendEvaluationResult.competition_id == filter_value
                        )

            query = query.order_by(BackendEvaluationResult.result_time.desc()).limit(
                limit
            )

            result = await session.execute(query)
            results = result.scalars().all()

            result_data = []
            for eval_result in results:
                data = eval_result.model_dump()
                # Convert datetime fields to ISO format
                for field in ["created_at", "updated_at", "result_time"]:
                    if field in data and data[field]:
                        data[field] = data[field].isoformat()
                result_data.append(data)

            return result_data

        except Exception as e:
            logger.error(f"Error getting initial evaluation data: {str(e)}")
            return []

    async def _get_initial_competition_data(
        self, session, filters: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Get active competitions."""
        try:
            # Competition is imported at module level

            query = select(Competition).where(Competition.active)

            # Apply filters if any
            if filters:
                for filter_key, filter_value in filters.items():
                    if filter_key == "competition_id":
                        query = query.where(Competition.id == filter_value)

            result = await session.execute(query)
            competitions = result.scalars().all()

            competition_data = []
            for competition in competitions:
                data = {
                    "id": competition.id,
                    "name": competition.name,
                    "description": competition.description,
                    "benchmarks": competition.benchmarks,
                    "points": competition.points,
                    "active": competition.active,
                    "start_time": competition.start_time.isoformat()
                    if competition.start_time
                    else None,
                    "end_time": competition.end_time.isoformat()
                    if competition.end_time
                    else None,
                    "created_at": competition.created_at.isoformat(),
                    "updated_at": competition.updated_at.isoformat(),
                }
                competition_data.append(data)

            return competition_data

        except Exception as e:
            logger.error(f"Error getting initial competition data: {str(e)}")
            return []


# Global broadcaster instance
event_broadcaster = RealtimeEventBroadcaster()
