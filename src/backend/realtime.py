"""
Real-time update system for Kinitro Backend.

This module provides WebSocket-based real-time updates to frontend clients,
including evaluation results, job status updates, episode data streaming, etc.
"""

import asyncio
import json
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List

from fastapi import WebSocket

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

    def __init__(self):
        self.client_connections: Dict[str, ClientConnection] = {}
        self._broadcast_queue: asyncio.Queue = asyncio.Queue()
        self._running = False
        self._broadcast_task = None

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


# Global broadcaster instance
event_broadcaster = RealtimeEventBroadcaster()
