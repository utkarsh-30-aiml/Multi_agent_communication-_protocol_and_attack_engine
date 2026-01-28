# agents/tourist_agent.py
"""
Robust Tourist Agent that works as both:
 - package: python -m CityBot2.agents.tourist_agent
 - direct:  python agents/tourist_agent.py
"""

import asyncio
import json
import os
import sys
import logging
from uuid import uuid4

import websockets
from websockets.exceptions import ConnectionClosedError, ConnectionClosedOK

# --- robust imports: prefer package-relative, fallback to project root ---
try:
    # when run as package
    from ..messages import make_envelope, dumps  # type: ignore
    from ..llm_client import ask_ollama  # type: ignore
except Exception:
    # fallback for direct script execution
    PROJECT_ROOT = os.path.dirname(os.path.dirname(__file__))  # CityBot2/
    if PROJECT_ROOT not in sys.path:
        sys.path.insert(0, PROJECT_ROOT)
    try:
        from messages import make_envelope, dumps  # type: ignore
    except Exception as e:
        raise ImportError(f"Could not import messages module. Ensure messages.py exists in project root. Err: {e}")
    try:
        from llm_client import ask_ollama  # type: ignore
    except Exception:
        # fallback stub if llm_client not present
        async def ask_ollama(model, prompt):
            await asyncio.sleep(0.1)
            return json.dumps({
                "spots": [{"name": "Beach A", "desc": "sandy beach"}],
                "foods": [{"name": "Cafe X", "desc": "seafood"}]
            })

# --- config & logging ---
GATEWAY_WS = os.getenv("GATEWAY_WS", "ws://localhost:8000/ws/tourist_agent")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "gemma3")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("tourist_agent")


# --- message handler ---
async def handle_task(msg: dict, ws: websockets.WebSocketClientProtocol):
    payload = msg.get("payload", {}) or {}
    constraints = payload.get("constraints", {})

    prompt = (
        f"Suggest top 5 tourist spots and top 5 food/restaurant recommendations for: {json.dumps(constraints)}. "
        "Return valid JSON with keys 'spots' and 'foods', each an array of {name, desc}."
    )

    try:
        resp = await ask_ollama(OLLAMA_MODEL, prompt)
    except Exception as e:
        logger.warning("ask_ollama failed: %s", e)
        resp = None

    result = None
    if isinstance(resp, dict):
        result = resp
    elif isinstance(resp, str):
        try:
            parsed = json.loads(resp)
            if isinstance(parsed, dict):
                result = parsed
        except Exception:
            result = None

    if not isinstance(result, dict):
        logger.info("Using fallback tourist suggestions")
        result = {
            "spots": [{"name": "Beach A", "desc": "sandy beach"}, {"name": "Old Fort", "desc": "historic fort"}],
            "foods": [{"name": "Cafe X", "desc": "seafood"}, {"name": "Street Stall Y", "desc": "local snacks"}]
        }

    prop = {
        "proposal_id": "p-" + uuid4().hex[:8],
        "type": "tourist",
        "options": result,
    }

    env = make_envelope(
        "PROPOSAL",
        "tourist_agent",
        "orchestrator",
        prop,
        session_id=msg.get("session_id"),
        session_token=msg.get("session_token"),
    )

    try:
        await ws.send(dumps(env))
        logger.info("Sent PROPOSAL for session %s", msg.get("session_id"))
    except Exception as e:
        logger.exception("Failed to send PROPOSAL: %s", e)


# --- main loop with reconnect/backoff ---
async def tourist_agent_loop():
    reconnect_delay = 1
    max_delay = 60

    while True:
        try:
            logger.info("Connecting to gateway %s ...", GATEWAY_WS)
            async with websockets.connect(
                GATEWAY_WS, 
                ping_interval=20, 
                ping_timeout=10,
                close_timeout=5
            ) as ws:
                reconnect_delay = 1
                
                # send CONNECT and wait for CONNECTED ack
                try:
                    await ws.send(json.dumps({"type": "CONNECT", "payload": {"client_type": "tourist_agent"}}))
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
            logger.exception("Unexpected error in tourist_agent_loop: %s", e)

        logger.info("Reconnecting in %ss...", reconnect_delay)
        await asyncio.sleep(reconnect_delay)
        reconnect_delay = min(reconnect_delay * 2, max_delay)


if __name__ == "__main__":
    try:
        asyncio.run(tourist_agent_loop())
    except KeyboardInterrupt:
        logger.info("Interrupted, exiting.")
