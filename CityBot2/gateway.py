# gateway.py
import os
import json
import uuid
import asyncio
import logging
from typing import Dict, Any

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from jose import jwt
import redis.asyncio as aioredis

# local helpers
from messages import dumps, make_envelope  # ensure messages.py is in the same package / sys.path

# --- config ---
JWT_SECRET = os.getenv("SESSION_JWT_SECRET", "dev_secret_change_me")
JWT_ALG = "HS256"
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
REDIS_SUBSCRIBE_PATTERN = "session:*"

# --- app + globals ---
app = FastAPI()
logger = logging.getLogger("gateway")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

redis_client: aioredis.Redis | None = None
connections: Dict[str, WebSocket] = {}  # client_id -> WebSocket

class ClientConnection:
    """Thread-safe wrapper for WebSocket connection state."""
    def __init__(self, websocket: WebSocket):
        self.websocket = websocket
        self.client_type: str | None = None
        self.connected = True
        self.send_lock = asyncio.Lock()  # Prevent concurrent sends

# --- helpers ---
def session_channel(session_id: str) -> str:
    return f"session:{session_id}"

def make_session_token(session_id: str, ttl_seconds: int = 900) -> str:
    payload = {"session_id": session_id}
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALG)

async def publish_session_message(session_id: str, msg: dict) -> None:
    """
    Publish a JSON-serializable message to the session channel.
    """
    if not redis_client:
        raise RuntimeError("Redis client not initialized")
    channel = session_channel(session_id)
    # redis-py asyncio publish is a coroutine
    await redis_client.publish(channel, json.dumps(msg))

async def send_to_client_safe(client_conn: ClientConnection, envelope: dict) -> bool:
    """
    Safely send a message to a connected client with proper locking.
    Returns True if successful, False if client is disconnected.
    """
    if not client_conn.connected:
        return False
    
    async with client_conn.send_lock:
        if not client_conn.connected:
            return False
        
        try:
            await asyncio.wait_for(
                client_conn.websocket.send_text(json.dumps(envelope)),
                timeout=3.0
            )
            return True
        except asyncio.TimeoutError:
            logger.warning("Timeout sending to client")
            client_conn.connected = False
            return False
        except RuntimeError as e:
            if "disconnect" in str(e).lower():
                logger.debug("Client already disconnected: %s", e)
            else:
                logger.warning("RuntimeError sending to client: %s", e)
            client_conn.connected = False
            return False
        except Exception as e:
            logger.debug("Error sending to client: %s", type(e).__name__)
            client_conn.connected = False
            return False

async def redis_subscriber():
    """
    Subscribes to pattern 'session:*' and forwards messages to local websocket clients.
    This runs in the background after startup.
    Uses send_to_client_safe to handle concurrent send/receive safely.
    """
    global redis_client
    if not redis_client:
        logger.error("redis_client not initialized in redis_subscriber")
        return

    # Use PubSub object for pattern subscription
    while True:
        pubsub = None
        try:
            pubsub = redis_client.pubsub(ignore_subscribe_messages=True)
            await pubsub.psubscribe(REDIS_SUBSCRIBE_PATTERN)
            logger.info("Subscribed to Redis pattern: %s", REDIS_SUBSCRIBE_PATTERN)

            async for message in pubsub.listen():
                # message example: {'type': 'pmessage', 'pattern': 'session:*', 'channel': 'session:uuid', 'data': '{"..."}'}
                if not message:
                    continue
                mtype = message.get("type")
                if mtype not in ("pmessage", "message"):
                    continue
                data = message.get("data")
                if not data:
                    continue

                # data should be a JSON string
                try:
                    envelope = json.loads(data)
                except Exception as e:
                    logger.warning("Invalid JSON in redis message: %s", str(data)[:200])
                    continue

                to = envelope.get("to")
                
                # Forward to matching local connections
                for cid in list(connections.keys()):
                    client_conn = connections.get(cid)
                    if not client_conn:
                        continue
                    
                    should_send = False
                    if to == "*" or to == cid or to == client_conn.client_type:
                        should_send = True
                    
                    if should_send:
                        success = await send_to_client_safe(client_conn, envelope)
                        if not success and cid in connections:
                            del connections[cid]

        except Exception as e:
            logger.exception("Redis subscriber error, will retry in 2s: %s", e)
            if pubsub:
                try:
                    await pubsub.close()
                except Exception:
                    pass
            await asyncio.sleep(2)

# --- FastAPI startup/shutdown events ---
@app.on_event("startup")
async def startup_event():
    global redis_client
    logger.info("Gateway startup: connecting to Redis at %s", REDIS_URL)
    # connect to redis with decode_responses so we receive strings not bytes
    redis_client = aioredis.from_url(REDIS_URL, decode_responses=True)
    # start subscriber background task
    asyncio.create_task(redis_subscriber())
    logger.info("Gateway startup complete")

@app.on_event("shutdown")
async def shutdown_event():
    global redis_client
    logger.info("Gateway shutdown: cleaning up connections and Redis")
    # close all websockets (best-effort)
    for cid, ws in list(connections.items()):
        try:
            await ws.close()
        except Exception:
            pass
    connections.clear()
    if redis_client:
        try:
            await redis_client.close()
        except Exception:
            pass

# --- WebSocket endpoint ---
@app.websocket("/ws/{client_id}")
async def ws_endpoint(websocket: WebSocket, client_id: str):
    """
    WebSocket endpoint for clients (user, orchestrator, agents).
    If a previous connection existed with same client_id, close it before registering.
    """
    await websocket.accept()

    # If a previous connection exists with same client_id, close it gracefully
    existing = connections.get(client_id)
    if existing is not None:
        try:
            logger.info("Closing previous connection for client_id=%s", client_id)
            existing.connected = False
            await existing.websocket.close()
        except Exception:
            logger.debug("Error closing previous connection for %s", client_id, exc_info=True)
        finally:
            connections.pop(client_id, None)

    # register new connection with proper wrapper
    client_conn = ClientConnection(websocket)
    connections[client_id] = client_conn
    logger.info("WebSocket connected: %s", client_id)

    try:
        while client_conn.connected:
            try:
                raw = await websocket.receive_text()
            except WebSocketDisconnect:
                logger.info("WebSocketDisconnect received from %s", client_id)
                break
            except RuntimeError as re:
                # Happens when receive() called after disconnect was already signalled
                logger.info("RuntimeError (disconnect race) from %s: %s", client_id, re)
                break
            except Exception as exc:
                logger.warning("Error while receiving from %s: %s", client_id, exc)
                break

            # process incoming text message
            try:
                msg = json.loads(raw)
            except Exception:
                # invalid JSON -> notify client and continue
                try:
                    async with client_conn.send_lock:
                        await websocket.send_text(json.dumps({"type": "ERROR", "message": "invalid_json"}))
                except Exception:
                    logger.warning("Failed to send invalid_json error to %s", client_id)
                continue

            msg_type = msg.get("type")

            # CONNECT: client identifies its type
            if msg_type == "CONNECT":
                ct = msg.get("payload", {}).get("client_type", "unknown")
                client_conn.client_type = ct
                try:
                    async with client_conn.send_lock:
                        await websocket.send_text(json.dumps({"type": "CONNECTED", "payload": {"client_id": client_id}}))
                except Exception:
                    logger.warning("Failed to send CONNECTED to %s", client_id)
                continue

            # START_SESSION: create a new ephemeral session and return token
            if msg_type == "START_SESSION":
                session_id = str(uuid.uuid4())
                token = make_session_token(session_id)
                try:
                    # persist small metadata in Redis for audit (best-effort)
                    if redis_client:
                        await redis_client.hset(f"session:{session_id}:meta", mapping={"owner": client_id})
                except Exception as e:
                    logger.warning("Failed to write session meta to Redis for %s: %s", session_id, e)

                try:
                    async with client_conn.send_lock:
                        await websocket.send_text(json.dumps({
                            "type": "SESSION_CREATED",
                            "session_id": session_id,
                            "session_token": token
                        }))
                except Exception:
                    logger.warning("Failed to send SESSION_CREATED to %s", client_id)
                continue

            # Generic session message routing: must contain session_id
            session_id = msg.get("session_id")
            if session_id:
                # publish session message to Redis
                try:
                    await publish_session_message(session_id, msg)
                except Exception as e:
                    logger.exception("Failed to publish session message for session %s from %s: %s", session_id, client_id, e)
                    # inform sender if possible
                    try:
                        async with client_conn.send_lock:
                            await websocket.send_text(json.dumps({"type": "ERROR", "message": "publish_failed"}))
                    except Exception:
                        pass
            else:
                # No session id in message
                try:
                    async with client_conn.send_lock:
                        await websocket.send_text(json.dumps({"type": "ERROR", "message": "no_session_id"}))
                except Exception:
                    logger.warning("Failed to send no_session_id error to %s", client_id)
    finally:
        # ensure the connection is cleaned up and not used by background tasks
        client_conn.connected = False
        if client_id in connections:
            try:
                del connections[client_id]
            except Exception:
                pass
        logger.info("WebSocket disconnected/cleaned: %s", client_id)
