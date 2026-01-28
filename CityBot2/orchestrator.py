# orchestrator.py
"""
Orchestrator (simple, no LangGraph dependency)

Run:
  python orchestrator.py
or (recommended when CityBot2 is a package):
  python -m CityBot2.orchestrator

Behavior:
- Connects to gateway WebSocket as "orchestrator"
- Receives INCOMING_QUERY messages from users (forwarded by gateway)
- Parses the query with ask_ollama into simple tasks (flight/hotel/tourist)
- Sends TASK messages to appropriate agents and waits for PROPOSALs
- Aggregates proposals and replies with AGGREGATED_RESULTS to the user
"""

import asyncio
import json
import os
import sys
import logging
from typing import Dict, Any, List
from uuid import uuid4

import websockets
import websockets.client

from websockets.exceptions import ConnectionClosedError, ConnectionClosedOK

# Robust imports for messages and llm_client (package or direct file)
try:
    # when run as a package: python -m CityBot2.orchestrator
    from ..messages import make_envelope, dumps  # type: ignore
    from ..llm_client import ask_ollama  # type: ignore
except Exception:
    # fallback: add project root to sys.path and import
    PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
    # project root is CityBot2 folder; ensure it is on sys.path so messages.py can be found
    parent = os.path.dirname(PROJECT_ROOT)
    if parent not in sys.path:
        sys.path.insert(0, parent)
    try:
        from messages import make_envelope, dumps  # type: ignore
    except Exception as e:
        raise ImportError(f"Could not import messages module. Ensure messages.py exists. Err: {e}")
    try:
        from llm_client import ask_ollama  # type: ignore
    except Exception:
        # fallback stub for ask_ollama
        async def ask_ollama(model: str, prompt: str, max_tokens: int = 512) -> str:
            await asyncio.sleep(0.1)
            # return a simple JSON string that indicates one flight + hotel + tourist task (demo)
            return json.dumps({"tasks": [{"type": "flight", "constraints": {"from": "BOM", "to": "GOI"}}, {"type": "hotel", "constraints": {"near": "beach"}}]})

# Config
GATEWAY_WS = os.getenv("GATEWAY_WS", "ws://localhost:8000/ws/orchestrator")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "llama3.2:1b")
PROPOSAL_WAIT_SECONDS = float(os.getenv("PROPOSAL_WAIT_SECONDS", "6.0"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("orchestrator")

# In-process store of queues to receive proposals per session
session_queues: Dict[str, asyncio.Queue] = {}


async def parse_user_query_to_tasks(query_text: str) -> List[Dict[str, Any]]:
    """
    Use LLM to parse the user query into a list of tasks:
    {"type": "...", "constraints": {...}}
    If parsing fails, return a fallback single flight task.
    """
    prompt = (
        "Parse this user travel request into a JSON object with a single key 'tasks' which is a list of "
        "objects with 'type' (one of: flight, hotel, tourist) and 'constraints' dict. "
        f"User request: {query_text}\n\nRespond ONLY with JSON."
    )
    try:
        parsed_text = await ask_ollama(OLLAMA_MODEL, prompt)
        # attempt to parse JSON
        doc = json.loads(parsed_text)
        tasks = doc.get("tasks") if isinstance(doc, dict) else None
        if isinstance(tasks, list):
            # basic validation
            cleaned = []
            for t in tasks:
                if isinstance(t, dict) and "type" in t:
                    cleaned.append({"type": t["type"], "constraints": t.get("constraints", {})})
            if cleaned:
                return cleaned
    except Exception as e:
        logger.warning("LLM parse failed or returned non-json: %s", e)

    # fallback default
    return [{"type": "flight", "constraints": {"from": "BOM", "to": "GOI"}}]


async def call_agent_for_task(session_id: str, session_token: str, websocket, task: dict):
    """
    Send a TASK envelope for a given task to the appropriate agent.
    """
    agent_map = {"flight": "flight_agent", "hotel": "hotel_agent", "tourist": "tourist_agent"}
    agent_name = agent_map.get(task.get("type"), "tourist_agent")
    payload = {"task": "search", "constraints": task.get("constraints", {})}
    env = make_envelope("TASK", "orchestrator", agent_name, payload, session_id=session_id, session_token=session_token)
    await websocket.send(dumps(env))
    logger.info("Sent TASK to %s for session %s", agent_name, session_id)


async def collect_proposals(session_id: str, wait_seconds: float) -> List[dict]:
    """
    Wait for proposals for the given session. Uses a per-session asyncio.Queue populated by the websocket receiver.
    Returns list of proposals received within wait_seconds.
    """
    q = session_queues.get(session_id)
    if q is None:
        q = asyncio.Queue()
        session_queues[session_id] = q

    proposals = []
    try:
        # first wait for at least one proposal with timeout
        first = await asyncio.wait_for(q.get(), timeout=wait_seconds)
        proposals.append(first)
    except asyncio.TimeoutError:
        return proposals

    # drain any other proposals available without blocking too long
    while True:
        try:
            item = q.get_nowait()
            proposals.append(item)
        except asyncio.QueueEmpty:
            break
    return proposals


async def handle_user_query(session_id: str, session_token: str, query_text: str, websocket):
    logger.info("Handling user query for session %s: %s", session_id, query_text)
    tasks = await parse_user_query_to_tasks(query_text)

    # ensure a queue exists to collect proposals
    session_queues.setdefault(session_id, asyncio.Queue())

    # send TASKs concurrently
    send_tasks = [asyncio.create_task(call_agent_for_task(session_id, session_token, websocket, t)) for t in tasks]
    await asyncio.gather(*send_tasks)

    # wait for proposals from agents
    proposals = await collect_proposals(session_id, PROPOSAL_WAIT_SECONDS)
    logger.info("Collected %d proposals for session %s", len(proposals), session_id)

    # Build a simple aggregated summary (very basic)
    summary_parts = []
    for p in proposals:
        # p is expected to be the payload of PROPOSAL envelope
        try:
            typ = p.get("type", "unknown")
            options = p.get("options") or p.get("results") or []
            if isinstance(options, list) and options:
                # pick the first option summary
                opt = options[0]
                if isinstance(opt, dict):
                    name = opt.get("name") or opt.get("provider") or opt.get("id") or str(opt)
                    price = opt.get("price")
                    if price:
                        summary_parts.append(f"{typ}: {name} @ {price}")
                    else:
                        summary_parts.append(f"{typ}: {name}")
        except Exception:
            continue

    if not summary_parts:
        summary = "No proposals found. Try adjusting your request."
    else:
        summary = "; ".join(summary_parts)

    # send aggregated results back to the user (to field set by gateway routing)
    reply = make_envelope("AGGREGATED_RESULTS", "orchestrator", "user", {"summary": summary}, session_id=session_id, session_token=session_token)
    await websocket.send(dumps(reply))
    logger.info("Sent AGGREGATED_RESULTS for session %s", session_id)


async def orchestrator_main():
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
                logger.info("Orchestrator connected to gateway")
                
                # announce
                try:
                    await ws.send(json.dumps({"type": "CONNECT", "payload": {"client_type": "orchestrator"}}))
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
                except Exception as e:
                    logger.warning("Error receiving CONNECTED ack: %s", e)

                backoff = 1.0  # reset backoff on successful connection

                # main receive loop
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

                    mtype = msg.get("type")
                    # PROPOSALs from agents: push into session queue
                    if mtype == "PROPOSAL":
                        sess = msg.get("session_id")
                        payload = msg.get("payload") or {}
                        if sess:
                            q = session_queues.setdefault(sess, asyncio.Queue())
                            # store payload (proposal body)
                            await q.put(payload)
                            logger.info("Stored PROPOSAL for session %s", sess)
                        else:
                            logger.warning("Received PROPOSAL without session_id: %r", msg)

                    # incoming queries forwarded from user
                    elif mtype == "INCOMING_QUERY":
                        session_id = msg.get("session_id")
                        session_token = msg.get("session_token")
                        query_text = msg.get("payload", {}).get("text", "")
                        # start background handler
                        asyncio.create_task(handle_user_query(session_id, session_token, query_text, ws))

                    elif mtype == "CONNECTED":
                        logger.info("Gateway acknowledged CONNECT")

                    else:
                        logger.debug("Unhandled message type: %s", mtype)

        except (ConnectionClosedError, ConnectionClosedOK) as e:
            logger.info("Websocket closed: %s", type(e).__name__)
        except OSError as e:
            logger.warning("OS error connecting to gateway: %s", e)
        except Exception as e:
            logger.exception("Unexpected error in orchestrator_main: %s", e)

        logger.info("Reconnecting in %.1fs...", backoff)
        await asyncio.sleep(backoff)
        backoff = min(max_backoff, backoff * 2)


if __name__ == "__main__":
    try:
        asyncio.run(orchestrator_main())
    except KeyboardInterrupt:
        logger.info("Orchestrator stopped by user")
