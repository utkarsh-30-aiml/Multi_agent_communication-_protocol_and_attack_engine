# agents/hotel_agent.py
"""
Robust Hotel Agent

- Works when executed as a package (recommended):
    python -m CityBot2.agents.hotel_agent

- Or directly for quick testing:
    python agents/hotel_agent.py

Imports `messages` and `llm_client` using a package-relative import when possible,
and falls back to adding the project root to sys.path for direct execution.
"""

import asyncio
import json
import os
import sys
import traceback
import logging
from uuid import uuid4

import websockets
from websockets.exceptions import ConnectionClosedError, ConnectionClosedOK

# ---- robust imports (package-relative preferred, fallback to project root) ----
try:
    # Preferred when running as a package
    from ..messages import make_envelope, dumps  # type: ignore
    from ..llm_client import ask_ollama  # type: ignore
except Exception:
    # Fallback when running the file directly (python agents/hotel_agent.py)
    PROJECT_ROOT = os.path.dirname(os.path.dirname(__file__))  # path/to/CityBot2
    if PROJECT_ROOT not in sys.path:
        sys.path.insert(0, PROJECT_ROOT)
    try:
        from messages import make_envelope, dumps  # type: ignore
    except Exception as e:
        raise ImportError(f"Could not import messages module. Ensure messages.py exists. Err: {e}")
    try:
        from llm_client import ask_ollama  # type: ignore
    except Exception:
        # Provide a simple fallback LLM stub if llm_client is not available
        async def ask_ollama(model, prompt):
            # Very small stub to simulate an LLM returning JSON string
            await asyncio.sleep(0.1)
            sample = [
                {"id": "h1", "name": "SeaView", "price": 3000, "location": "Beach"},
                {"id": "h2", "name": "BudgetInn", "price": 1500, "location": "Near Market"},
                {"id": "h3", "name": "CityCenter", "price": 2500, "location": "Downtown"},
            ]
            return json.dumps(sample)

# ---- config / logger ----
GATEWAY_WS = os.getenv("GATEWAY_WS", "ws://localhost:8000/ws/hotel_agent")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "gemma3")

DEFAULT_OPTIONS = [
    {"id": "h1", "name": "SeaView", "price": 3000, "location": "Beach"},
    {"id": "h2", "name": "BudgetInn", "price": 1500, "location": "Near Market"},
    {"id": "h3", "name": "CityCenter", "price": 2500, "location": "Downtown"},
]

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("hotel_agent")


# ---- handlers ----
async def handle_task(msg: dict, ws: websockets.WebSocketClientProtocol):
    """Handle incoming TASK messages: call LLM to generate hotel options and send PROPOSAL."""
    payload = msg.get("payload", {}) or {}
    constraints = payload.get("constraints", {})

    prompt = (
        f"Find 3 hotels matching these constraints: {json.dumps(constraints)}.\n"
        "Return a JSON array of objects with keys: id, name, price, location."
    )

    try:
        resp = await ask_ollama(OLLAMA_MODEL, prompt)
    except Exception as e:
        logger.warning("ask_ollama error: %s", e)
        resp = None

    opts = None

    # If the LLM returns Python objects already
    if isinstance(resp, (list, dict)):
        opts = resp if isinstance(resp, list) else [resp]
    else:
        # try parse JSON string
        if isinstance(resp, str):
            try:
                parsed = json.loads(resp)
                opts = parsed if isinstance(parsed, list) else [parsed]
            except Exception:
                opts = None

    # Validate options; fall back to defaults if invalid
    if not isinstance(opts, list) or not all(isinstance(o, dict) for o in opts):
        logger.info("LLM response invalid or unparsable; falling back to DEFAULT_OPTIONS.")
        opts = DEFAULT_OPTIONS

    prop = {
        "proposal_id": "p-" + uuid4().hex[:8],
        "type": "hotel",
        "options": opts,
    }

    envelope = make_envelope(
        "PROPOSAL",
        "hotel_agent",
        "orchestrator",
        prop,
        session_id=msg.get("session_id"),
        session_token=msg.get("session_token"),
    )

    try:
        await ws.send(dumps(envelope))
        logger.info("Sent PROPOSAL for session %s", msg.get("session_id"))
    except Exception as e:
        logger.exception("Failed to send PROPOSAL: %s", e)


async def handle_booking_request(msg: dict, ws: websockets.WebSocketClientProtocol):
    """Handle BOOKING_REQUEST by returning a stub BOOKING_RESULT (replace with real API calls)."""
    payload = msg.get("payload", {}) or {}
    booking = {
        "status": "confirmed",
        "booking_url": "https://example.com/hotel/1",
        "reservation_id": "R" + uuid4().hex[:6],
        "requested": payload,
    }
    envelope = make_envelope(
        "BOOKING_RESULT",
        "hotel_agent",
        "orchestrator",
        booking,
        session_id=msg.get("session_id"),
        session_token=msg.get("session_token"),
    )
    try:
        await ws.send(dumps(envelope))
        logger.info("Sent BOOKING_RESULT for session %s", msg.get("session_id"))
    except Exception as e:
        logger.exception("Failed to send BOOKING_RESULT: %s", e)


# ---- main agent loop ----
async def hotel_agent_loop():
    reconnect_delay = 1
    max_reconnect = 60

    while True:
        try:
            logger.info("Connecting to gateway %s ...", GATEWAY_WS)
            async with websockets.connect(
                GATEWAY_WS, 
                ping_interval=20, 
                ping_timeout=10,
                close_timeout=5
            ) as ws:
                reconnect_delay = 1  # reset on successful connect
                
                # send CONNECT envelope and wait for CONNECTED ack
                try:
                    await ws.send(json.dumps({"type": "CONNECT", "payload": {"client_type": "hotel_agent"}}))
                    logger.info("Sent CONNECT message")
                except Exception as e:
                    logger.warning("Failed to send CONNECT: %s", e)
                    raise

                # wait for CONNECTED ack
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=5.0)
                    try:
                        ack = json.loads(raw)
                        if ack.get("type") == "CONNECTED":
                            logger.info("Received CONNECTED ack from gateway")
                        else:
                            logger.info("Expected CONNECTED but got: %s", ack.get("type"))
                    except json.JSONDecodeError:
                        logger.warning("Non-JSON response after CONNECT")
                except asyncio.TimeoutError:
                    logger.warning("Timeout waiting for CONNECTED ack")
                    raise
                except Exception as e:
                    logger.warning("Error receiving CONNECTED ack: %s", e)
                    raise

                # receive loop with explicit receive_text instead of async for
                while True:
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=60.0)
                    except asyncio.TimeoutError:
                        logger.debug("No message for 60s (keep-alive working)")
                        continue
                    except (ConnectionClosedError, ConnectionClosedOK) as e:
                        logger.info("Connection closed: %s", type(e).__name__)
                        raise
                    except RuntimeError as re:
                        logger.info("RuntimeError during receive: %s", re)
                        raise
                    except Exception as e:
                        logger.warning("Error during receive: %s", e)
                        raise

                    try:
                        msg = json.loads(raw)
                    except json.JSONDecodeError:
                        logger.warning("Received non-JSON message: %r", raw[:100])
                        continue

                    if not isinstance(msg, dict):
                        continue

                    mtype = msg.get("type")
                    try:
                        if mtype == "TASK":
                            await handle_task(msg, ws)
                        elif mtype == "BOOKING_REQUEST":
                            await handle_booking_request(msg, ws)
                        else:
                            logger.debug("Unhandled message type: %s", mtype)
                    except (ConnectionClosedError, ConnectionClosedOK) as e:
                        logger.warning("Connection closed while handling message: %s", e)
                        raise
                    except Exception:
                        logger.exception("Error handling message")
                        # continue processing

        except (ConnectionClosedError, ConnectionClosedOK):
            logger.info("WebSocket connection closed")
        except (OSError, ConnectionRefusedError) as e:
            logger.warning("Connection error: %s. Reconnecting in %ss...", e, reconnect_delay)
        except Exception as e:
            logger.exception("Unexpected error in hotel_agent_loop: %s", e)

        # backoff before reconnecting
        logger.info("Reconnecting in %ss...", reconnect_delay)
        await asyncio.sleep(reconnect_delay)
        reconnect_delay = min(reconnect_delay * 2, max_reconnect)


if __name__ == "__main__":
    try:
        asyncio.run(hotel_agent_loop())
    except KeyboardInterrupt:
        logger.info("Interrupted, exiting.")
