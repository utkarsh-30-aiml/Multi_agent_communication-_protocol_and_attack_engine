# user_sim.py
"""
Hardened user simulator.
Run this after the gateway is running.
"""

import asyncio
import json
import os
import sys
import logging

# robust import for messages
try:
    from .messages import make_envelope, dumps  # when running as module
except Exception:
    PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
    parent = os.path.dirname(PROJECT_ROOT)
    if parent not in sys.path:
        sys.path.insert(0, parent)
    from messages import make_envelope, dumps  # fallback

import websockets
from websockets.exceptions import ConnectionClosedError, ConnectionClosedOK

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("user_sim")

GATEWAY_WS = os.getenv("GATEWAY_WS", "ws://localhost:8000/ws/user123")
RECV_TIMEOUT = float(os.getenv("USER_SIM_RECV_TIMEOUT", "30.0"))  # increased from 10s

async def recv_json_with_timeout(ws, timeout=RECV_TIMEOUT):
    try:
        raw = await asyncio.wait_for(ws.recv(), timeout=timeout)
    except asyncio.TimeoutError:
        return None
    except (ConnectionClosedError, ConnectionClosedOK):
        return None
    try:
        return json.loads(raw)
    except Exception:
        return None

async def main():
    uri = GATEWAY_WS
    logger.info("Connecting to %s", uri)
    try:
        async with websockets.connect(
            uri, 
            ping_interval=20, 
            ping_timeout=10,
            close_timeout=5
        ) as ws:
            # 1) CONNECT
            await ws.send(json.dumps({"type":"CONNECT","payload":{"client_type":"user"}}))
            logger.info("Sent CONNECT")

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
            except Exception as e:
                logger.warning("Error receiving CONNECTED ack: %s", e)

            # 2) START_SESSION
            await ws.send(json.dumps({"type":"START_SESSION"}))
            logger.info("Sent START_SESSION, waiting for SESSION_CREATED...")

            resp = await recv_json_with_timeout(ws, timeout=RECV_TIMEOUT)
            if not resp:
                logger.error("Did not receive SESSION_CREATED within %ss; exiting.", RECV_TIMEOUT)
                return

            if resp.get("type") != "SESSION_CREATED":
                logger.warning("Expected SESSION_CREATED but got: %r", resp)

            session_id = resp.get("session_id")
            token = resp.get("session_token")
            logger.info("Session created: %s", session_id)

            # 3) Send INCOMING_QUERY
            user_text = "I want a hotel for 3 days in Goa Aug for 3 days, 10-12"
            payload = {"text": user_text}
            query_msg = make_envelope("INCOMING_QUERY", "user:user123", "orchestrator", payload, session_id=session_id, session_token=token)
            await ws.send(dumps(query_msg))
            logger.info("Sent INCOMING_QUERY")

            # 4) Listen for messages until AGGREGATED_RESULTS arrives (or timeout)
            while True:
                msg = await recv_json_with_timeout(ws, timeout=60.0)
                if msg is None:
                    logger.info("No message received in last 60s — still waiting...")
                    continue
                logger.info("USER RECV: %s", msg)
                if msg.get("type") == "AGGREGATED_RESULTS":
                    logger.info("Received AGGREGATED_RESULTS — done.")
                    break

    except (ConnectionClosedError, ConnectionClosedOK) as e:
        logger.warning("Connection closed: %s", type(e).__name__)
    except Exception as e:
        logger.exception("user_sim error: %s", e)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Interrupted by user, exiting.")
