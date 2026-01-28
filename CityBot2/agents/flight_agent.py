# agents/flight_agent.py (corrected)
"""
Robust flight agent that reconnects to gateway on failure.
"""
import asyncio
import json
import os
import sys
import logging
from uuid import uuid4

# import messages helper robustly
try:
    from ..messages import make_envelope, dumps
except Exception:
    # fallback for direct script execution
    PROJECT_ROOT = os.path.dirname(os.path.dirname(__file__))
    if PROJECT_ROOT not in sys.path:
        sys.path.insert(0, PROJECT_ROOT)
    from messages import make_envelope, dumps

import websockets
from websockets.exceptions import ConnectionClosedError, ConnectionClosedOK, InvalidHandshake

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("flight_agent")

GATEWAY_WS = os.getenv("GATEWAY_WS", "ws://localhost:8000/ws/flight_agent")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "gemma3")

# simple stub for LLM call (replace with your actual llm_client.ask_ollama)
async def ask_ollama_stub(model, prompt):
    await asyncio.sleep(0.2)
    return json.dumps([
        {"id":"fl1","price":4200,"provider":"AirDemo","departure":"08:00","arrival":"09:10"},
        {"id":"fl2","price":5200,"provider":"BudgetAir","departure":"06:00","arrival":"07:30"}
    ])

# Use the real llm_client if available
try:
    from ..llm_client import ask_ollama
except Exception:
    try:
        from llm_client import ask_ollama  # fallback
    except Exception:
        ask_ollama = ask_ollama_stub

async def handle_message(ws, msg):
    typ = msg.get("type")
    if typ == "TASK":
        payload = msg.get("payload", {})
        constraints = payload.get("constraints", {})
        prompt = f"Given constraints {constraints}, propose up to 3 flight options as JSON array with id, price, provider, departure, arrival."
        try:
            resp_text = await ask_ollama(OLLAMA_MODEL, prompt)
            options = json.loads(resp_text)
        except Exception:
            options = [
                {"id":"fl1","price":4200,"provider":"AirDemo","departure":"08:00","arrival":"09:10"},
                {"id":"fl2","price":5200,"provider":"BudgetAir","departure":"06:00","arrival":"07:30"}
            ]
        prop = {"proposal_id": "p-" + uuid4().hex[:8], "type": "flight", "options": options}
        env = make_envelope("PROPOSAL", "flight_agent", "orchestrator", prop, session_id=msg.get("session_id"), session_token=msg.get("session_token"))
        try:
            await ws.send(dumps(env))
            logger.info("Sent PROPOSAL for session %s", msg.get("session_id"))
        except Exception:
            logger.exception("Failed to send PROPOSAL")

    elif typ == "BOOKING_REQUEST":
        booking = {"status":"confirmed", "ticket_url":"https://example.com/ticket/abc", "pnr":"PNR123"}
        env = make_envelope("BOOKING_RESULT", "flight_agent", "orchestrator", booking, session_id=msg.get("session_id"), session_token=msg.get("session_token"))
        try:
            await ws.send(dumps(env))
            logger.info("Sent BOOKING_RESULT for session %s", msg.get("session_id"))
        except Exception:
            logger.exception("Failed to send BOOKING_RESULT")

    else:
        logger.debug("Unhandled message type: %s", typ)


async def flight_agent_loop():
    backoff = 1.0
    max_backoff = 30.0
    while True:
        try:
            logger.info("Connecting to gateway %s", GATEWAY_WS)
            async with websockets.connect(
                GATEWAY_WS, 
                ping_interval=20, 
                ping_timeout=10,
                close_timeout=5
            ) as ws:
                logger.info("Connected to gateway")

                # send initial CONNECT and wait for CONNECTED ack
                try:
                    connect_msg = {"type": "CONNECT", "payload": {"client_type": "flight_agent"}}
                    await ws.send(json.dumps(connect_msg))
                    logger.info("Sent CONNECT message")
                except Exception as e:
                    logger.warning("Failed to send CONNECT: %s", e)
                    raise

                # wait for CONNECTED ack with timeout
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
                    logger.warning("Timeout waiting for CONNECTED ack from gateway")
                    raise
                except Exception as e:
                    logger.warning("Error receiving CONNECTED ack: %s", e)
                    raise

                backoff = 1.0  # reset backoff on successful connection

                # Now stable receive loop
                while True:
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=60.0)
                    except asyncio.TimeoutError:
                        # Just log, don't disconnect - keep-alive pings should prevent this
                        logger.debug("No message received for 60s (keep-alive working)")
                        continue
                    except (ConnectionClosedError, ConnectionClosedOK) as e:
                        logger.info("Connection closed: %s", e)
                        raise
                    except RuntimeError as re:
                        logger.info("RuntimeError during receive: %s", re)
                        raise
                    except Exception as e:
                        logger.exception("Unexpected error during receive: %s", e)
                        raise

                    try:
                        msg = json.loads(raw)
                    except json.JSONDecodeError:
                        logger.warning("Invalid JSON received: %r", raw[:100])
                        continue

                    # handle message
                    try:
                        await handle_message(ws, msg)
                    except (ConnectionClosedError, ConnectionClosedOK):
                        logger.warning("Connection closed while handling message")
                        raise
                    except Exception:
                        logger.exception("Error handling message")
                        # Continue loop, don't close connection

        except (ConnectionClosedError, ConnectionClosedOK) as e:
            logger.info("WebSocket closed by server/client: %s", type(e).__name__)
        except InvalidHandshake as e:
            logger.error("Invalid websocket handshake: %s", e)
        except OSError as e:
            logger.error("OS error connecting to gateway: %s", e)
        except Exception as e:
            logger.exception("Unexpected error in flight_agent_loop: %s", e)

        # Exponential backoff before reconnect
        logger.info("Reconnecting in %.1fs...", backoff)
        await asyncio.sleep(backoff)
        backoff = min(max_backoff, backoff * 2)


if __name__ == "__main__":
    try:
        asyncio.run(flight_agent_loop())
    except KeyboardInterrupt:
        logger.info("Shutting down flight agent")
