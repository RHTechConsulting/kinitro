"""Parent validator job broadcaster for distributing jobs to child validators."""

import asyncio
import json
import uuid
from datetime import datetime, timedelta, timezone
from typing import Dict, Set

import websockets
from websockets.exceptions import ConnectionClosedError, ConnectionClosedOK
from websockets.server import WebSocketServerProtocol

from core.log import get_logger
from core.schemas import ChainCommitmentResponse

from .job_distribution import (
    ChildRegisterMessage,
    HeartbeatMessage,
    JobAssignmentMessage,
    MessageType,
    ValidatorConnectionInfo,
    ValidatorMessage,
)

logger = get_logger(__name__)


class ParentValidatorBroadcaster:
    """Manages WebSocket connections and job distribution to child validators."""

    def __init__(self, host: str = "localhost", port: int = 8765):
        self.host = host
        self.port = port
        self.connected_children: Dict[str, ValidatorConnectionInfo] = {}
        self.websocket_connections: Dict[str, WebSocketServerProtocol] = {}
        self.server = None
        self._running = False
        self._heartbeat_task = None

    async def start_server(self):
        """Start the WebSocket server for child validator connections."""
        logger.info(f"Starting parent validator broadcaster on {self.host}:{self.port}")

        self.server = await websockets.serve(
            self._handle_connection,
            self.host,
            self.port,
            ping_interval=30,
            ping_timeout=10,
        )

        self._running = True
        self._heartbeat_task = asyncio.create_task(self._heartbeat_monitor())
        logger.info("Parent validator broadcaster started successfully")

    async def stop_server(self):
        """Stop the WebSocket server and close all connections."""
        logger.info("Stopping parent validator broadcaster")
        self._running = False

        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass

        if self.server:
            self.server.close()
            await self.server.wait_closed()

        # Close all child connections
        for websocket in self.websocket_connections.values():
            await websocket.close()

        self.connected_children.clear()
        self.websocket_connections.clear()
        logger.info("Parent validator broadcaster stopped")

    async def _handle_connection(self, websocket: WebSocketServerProtocol):
        """Handle a new WebSocket connection from a child validator."""
        client_id = f"{websocket.remote_address[0]}:{websocket.remote_address[1]}"
        logger.info(f"New connection from {client_id}")

        try:
            async for message in websocket:
                await self._process_message(websocket, client_id, message)
        except (ConnectionClosedError, ConnectionClosedOK):
            logger.info(f"Child validator {client_id} disconnected")
        except Exception as e:
            logger.error(f"Error handling connection from {client_id}: {e}")
        finally:
            await self._remove_child(client_id)

    async def _process_message(
        self, websocket: WebSocketServerProtocol, client_id: str, raw_message: str
    ):
        """Process incoming messages from child validators."""
        try:
            message_data = json.loads(raw_message)
            message_type = MessageType(message_data.get("message_type"))

            if message_type == MessageType.CHILD_REGISTER:
                await self._handle_child_registration(
                    websocket, client_id, message_data
                )
            elif message_type == MessageType.HEARTBEAT:
                await self._handle_heartbeat(client_id, message_data)
            elif message_type == MessageType.STATUS_UPDATE:
                await self._handle_status_update(client_id, message_data)
            else:
                logger.warning(f"Unknown message type from {client_id}: {message_type}")

        except Exception as e:
            logger.error(f"Error processing message from {client_id}: {e}")

    async def _handle_child_registration(
        self, websocket: WebSocketServerProtocol, client_id: str, message_data: dict
    ):
        """Handle child validator registration."""
        try:
            register_msg = ChildRegisterMessage.model_validate(message_data)

            # Store connection info
            connection_info = ValidatorConnectionInfo(
                validator_id=client_id,
                hotkey=register_msg.validator_hotkey,
                connected_at=datetime.now(timezone.utc),
                last_heartbeat=datetime.now(timezone.utc),
            )

            self.connected_children[client_id] = connection_info
            self.websocket_connections[client_id] = websocket

            logger.info(
                f"Child validator registered: {register_msg.validator_hotkey} ({client_id})"
            )

            # Send acknowledgment
            ack_message = HeartbeatMessage(
                sender_id="parent",
                message_id=str(uuid.uuid4()),
                status="registered",
            )
            await self._send_message(websocket, ack_message)

        except Exception as e:
            logger.error(f"Error handling child registration from {client_id}: {e}")

    async def _handle_heartbeat(self, client_id: str, message_data: dict):
        """Handle heartbeat messages from child validators."""
        try:
            heartbeat_msg = HeartbeatMessage.model_validate(message_data)

            if client_id in self.connected_children:
                self.connected_children[client_id].last_heartbeat = datetime.now(
                    timezone.utc
                )
                self.connected_children[client_id].status = heartbeat_msg.status

                if heartbeat_msg.queue_size is not None:
                    self.connected_children[
                        client_id
                    ].queue_size = heartbeat_msg.queue_size

                logger.debug(f"Heartbeat received from {client_id}")

        except Exception as e:
            logger.error(f"Error handling heartbeat from {client_id}: {e}")

    async def _handle_status_update(self, client_id: str, message_data: dict):
        """Handle status updates from child validators."""
        try:
            logger.info(f"Status update from {client_id}: {message_data}")
        except Exception as e:
            logger.error(f"Error handling status update from {client_id}: {e}")

    async def _remove_child(self, client_id: str):
        """Remove a child validator from tracking."""
        if client_id in self.connected_children:
            hotkey = self.connected_children[client_id].hotkey
            del self.connected_children[client_id]
            logger.info(f"Removed child validator: {hotkey} ({client_id})")

        if client_id in self.websocket_connections:
            del self.websocket_connections[client_id]

    async def broadcast_job(self, commitment: ChainCommitmentResponse) -> int:
        """Broadcast a job assignment to all connected child validators."""
        if not self.connected_children:
            logger.warning("No child validators connected - cannot broadcast job")
            return 0

        job_message = JobAssignmentMessage(
            sender_id="parent",
            message_id=str(uuid.uuid4()),
            commitment=commitment,
        )

        broadcast_count = 0
        failed_connections = []

        for client_id, websocket in self.websocket_connections.items():
            try:
                await self._send_message(websocket, job_message)
                broadcast_count += 1
                logger.debug(f"Job broadcasted to child validator {client_id}")
            except Exception as e:
                logger.error(f"Failed to send job to child validator {client_id}: {e}")
                failed_connections.append(client_id)

        # Clean up failed connections
        for client_id in failed_connections:
            await self._remove_child(client_id)

        logger.info(f"Job broadcasted to {broadcast_count} child validators")
        return broadcast_count

    async def _send_message(
        self, websocket: WebSocketServerProtocol, message: ValidatorMessage
    ):
        """Send a message to a WebSocket connection."""
        try:
            message_json = message.model_dump_json()
            await websocket.send(message_json)
        except Exception as e:
            logger.error(f"Failed to send message: {e}")
            raise

    async def _heartbeat_monitor(self):
        """Monitor child validator heartbeats and remove stale connections."""
        while self._running:
            try:
                current_time = datetime.now(timezone.utc)
                stale_connections = []

                for client_id, connection_info in self.connected_children.items():
                    time_since_heartbeat = current_time - connection_info.last_heartbeat

                    if time_since_heartbeat > timedelta(minutes=2):  # 2 minute timeout
                        stale_connections.append(client_id)

                # Remove stale connections
                for client_id in stale_connections:
                    logger.warning(
                        f"Removing stale child validator connection: {client_id}"
                    )
                    if client_id in self.websocket_connections:
                        try:
                            await self.websocket_connections[client_id].close()
                        except Exception:
                            pass
                    await self._remove_child(client_id)

                await asyncio.sleep(30)  # Check every 30 seconds

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in heartbeat monitor: {e}")
                await asyncio.sleep(30)

    def get_connected_children(self) -> Dict[str, ValidatorConnectionInfo]:
        """Get information about all connected child validators."""
        return self.connected_children.copy()

    def get_active_child_count(self) -> int:
        """Get the count of active child validators."""
        return len(
            [
                child
                for child in self.connected_children.values()
                if child.status == "active"
            ]
        )
