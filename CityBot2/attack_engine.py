"""
attack_engine.py

Usage:
    python attack_engine.py

Configuration (top-of-file): set GATEWAY_WS, DEFAULT_SESSION_ID/TOKEN or pass via CLI/env.

Features:
 - Connects to gateway WebSocket and can target 'orchestrator' or other endpoints
 - Implements attack types: impersonate, flooding, manipulation, state, reply, fakelink, replay
 - Logs each attack to JSONL and CSV for dataset creation
 - Accepts multiple sessions to attack (list)
 - Parameters are configurable (intervals, flood_rate, weights)
"""

import asyncio
import json
import os
import random
import sys
import csv
import time
import traceback
from datetime import datetime
from typing import Dict, Any, Optional, List
import websockets


# Try to reuse project messages helper if available
try:
    # If you put this file in the project root it should find CityBot2.messages
    from CityBot2.messages import make_envelope  # type: ignore
    def _make_env(type_, from_, to, payload, session_id=None, session_token=None):
        return make_envelope(type_, from_, to, payload, session_id=session_id, session_token=session_token)
except Exception:
    # fallback - minimal compatible envelope maker
    import uuid, time
    def _now_ts() -> int:
        return int(time.time() * 1000)
    def _make_env(type_, from_, to, payload, session_id=None, session_token=None):
        return {
            "type": type_,
            "session_id": session_id,
            "session_token": session_token,
            "from": from_,
            "to": to,
            "message_id": str(uuid.uuid4()),
            "timestamp": _now_ts(),
            "payload": payload
        }

# websocket client lib
try:
    import websockets
except Exception:
    print("Please install websockets: pip install websockets")
    raise

# ---------------- CONFIG ----------------
GATEWAY_WS = os.getenv("GATEWAY_WS", "ws://127.0.0.1:8000/ws")  # adjust to your gateway URL
DEFAULT_SESSIONS = [
    # list of (session_id, session_token) pairs to attack
    ("test-session-1", "token-abc-123"),
]
LOG_DIR = os.getenv("ATTACK_LOG_DIR", "./attack_logs")
JSONL_FILE = os.path.join(LOG_DIR, "attacks_log.jsonl")
CSV_FILE = os.path.join(LOG_DIR, "attacks_summary.csv")

# Attack scheduling and weights
ATTACK_WEIGHTS = {
    "impersonate": 1.5,
    "flooding": 1.2,
    "manipulation": 1.0,
    "state": 0.8,
    "reply": 1.0,
    "fakelink": 0.6,
    "replay": 0.9
}
DEFAULT_INTERVAL_RANGE = (2.0, 8.0)  # seconds between attacks (randomized)
FLOOD_BATCH = 25  # messages in a flooding burst (configurable)
FLOOD_INTERVAL = 0.05  # seconds between flood messages

# Endpoints that can be targeted. These should match message 'to' used by your orchestrator/gateway.
ENDPOINTS = ["orchestrator", "gateway", "user", "agent.flight", "agent.hotel", "agent.tourist"]

# ----------------- Utilities -----------------
def ensure_logdir():
    os.makedirs(LOG_DIR, exist_ok=True)
    # ensure CSV has header
    if not os.path.exists(CSV_FILE):
        with open(CSV_FILE, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([
                "timestamp_iso", "session_id", "attack_type", "target_to", "from_field",
                "message_type", "message_summary", "outcome", "response_summary"
            ])

def now_iso():
    return datetime.utcnow().isoformat() + "Z"

def mask_token(t: Optional[str]) -> Optional[str]:
    if not t:
        return None
    if len(t) <= 8:
        return t[:2] + "***"
    return t[:4] + "..." + t[-4:]

def log_attack(record: Dict[str, Any]):
    # append JSONL
    with open(JSONL_FILE, "a") as f:
        f.write(json.dumps(record) + "\n")
    # append CSV summary
    with open(CSV_FILE, "a", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            record.get("timestamp_iso"),
            record.get("session_id"),
            record.get("attack_type"),
            record.get("target_to"),
            record.get("from_field"),
            record.get("message_type"),
            record.get("message_summary"),
            record.get("outcome"),
            record.get("response_summary")
        ])

# Helper to summarize payload for CSV
def summarize_payload(payload):
    if isinstance(payload, dict):
        keys = list(payload.keys())
        return f"keys={keys[:4]}"
    return str(payload)[:200]

# ---------------- Attack implementations ----------------
class AttackEngine:
    def __init__(self, gateway_ws: str, sessions: List[Dict[str,str]]):
        self.gateway_ws = gateway_ws
        self.sessions = sessions
        self.conn = None  # websockets connection
        self.recv_task = None
        self.replay_store = []  # store observed envelopes for replay attacks
        ensure_logdir()

    async def connect(self):
        # connect and keep connection alive
        try:
            self.conn = await websockets.connect(self.gateway_ws, ping_interval=20, ping_timeout=10, close_timeout=5)
            print(f"[attack] connected to {self.gateway_ws}")
            # start receiver
            self.recv_task = asyncio.create_task(self.receiver_loop())
        except Exception as e:
            print("connection error:", e)
            raise

    async def close(self):
        if self.recv_task:
            self.recv_task.cancel()
        if self.conn:
            await self.conn.close()

    async def receiver_loop(self):
        try:
            async for msg in self.conn:
                # try parse JSON
                try:
                    obj = json.loads(msg)
                except Exception:
                    obj = {"raw": msg}
                # store for potential replay and debugging
                self.replay_store.append(obj)
                # keep replay store limited
                if len(self.replay_store) > 1000:
                    self.replay_store.pop(0)
                # print a short summary for visibility
                print("[attack][recv] ", obj.get("type", "RAW"), obj.get("from"), obj.get("to"))
        except asyncio.CancelledError:
            return
        except Exception as e:
            print("receiver loop error:", e)
            traceback.print_exc()

    async def send_envelope(self, envelope: Dict[str,Any]):
        raw = json.dumps(envelope)
        try:
            await self.conn.send(raw)
            return {"ok": True}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    def choose_target_endpoint(self):
        # pick a target from available endpoints
        return random.choice(ENDPOINTS)

    def choose_from_field(self, attack_type, session_id):
        # For impersonation attacks we may attempt to forge
        if attack_type == "impersonate":
            # pick something to impersonate
            candidates = ["user", "agent.flight", "agent.hotel", "agent.tourist", "gateway"]
            return random.choice(candidates)
        # otherwise use the session's actor or random
        # sessions in DEFAULT_SESSIONS are likely user sessions
        return f"attacker-{random.randint(100,999)}"

    async def run_attack_cycle(self, session_id: str, session_token: str, runtime_seconds: Optional[int] = None):
        start = time.time()
        while True:
            if runtime_seconds and (time.time() - start) > runtime_seconds:
                print("[attack] runtime complete")
                break
            # pick attack type using weights
            atype = random.choices(list(ATTACK_WEIGHTS.keys()), weights=list(ATTACK_WEIGHTS.values()))[0]
            try:
                if atype == "impersonate":
                    await self.attack_impersonate(session_id, session_token)
                elif atype == "flooding":
                    await self.attack_flooding(session_id, session_token)
                elif atype == "manipulation":
                    await self.attack_manipulation(session_id, session_token)
                elif atype == "state":
                    await self.attack_state(session_id, session_token)
                elif atype == "reply":
                    await self.attack_reply(session_id, session_token)
                elif atype == "fakelink":
                    await self.attack_fakelink(session_id, session_token)
                elif atype == "replay":
                    await self.attack_replay(session_id, session_token)
                else:
                    print("Unknown attack type", atype)
            except Exception as e:
                print("Error running attack", atype, e)
                traceback.print_exc()

            # randomized wait between attacks (except floods which manage their own pacing)
            wait = random.uniform(*DEFAULT_INTERVAL_RANGE)
            await asyncio.sleep(wait)

    # --- attack implementations ---
    async def attack_impersonate(self, session_id, session_token):
        target = self.choose_target_endpoint()
        forged_from = self.choose_from_field("impersonate", session_id)
        # craft an INCOMING_QUERY that pretends to be another user/agent
        payload = {"query": f"Forged message from {forged_from}", "meta": {"attack":"impersonate"}}
        envelope = _make_env("INCOMING_QUERY", forged_from, target, payload, session_id=session_id, session_token=session_token)
        res = await self.send_envelope(envelope)
        rec = {
            "timestamp_iso": now_iso(),
            "session_id": session_id,
            "session_token_masked": mask_token(session_token),
            "attack_type": "impersonate",
            "target_to": target,
            "from_field": forged_from,
            "message_type": envelope.get("type"),
            "message_summary": summarize_payload(payload),
            "raw_message": envelope,
            "outcome": res,
            "response_summary": None
        }
        log_attack(rec)
        print("[attack][impersonate] sent to", target, "from", forged_from)

    async def attack_flooding(self, session_id, session_token):
        target = self.choose_target_endpoint()
        msg_type = random.choice(["INCOMING_QUERY", "TASK", "PROPOSAL"])
        payload_template = {"attack":"flooding","seq":None,"note":"flood_burst"}
        outcomes = []
        for i in range(FLOOD_BATCH):
            payload = dict(payload_template)
            payload["seq"] = i
            envelope = _make_env(msg_type, f"flooder-{random.randint(1,999)}", target, payload, session_id=session_id, session_token=session_token)
            res = await self.send_envelope(envelope)
            outcomes.append(res)
            # small spacing to allow gateway to process
            await asyncio.sleep(FLOOD_INTERVAL)
        rec = {
            "timestamp_iso": now_iso(),
            "session_id": session_id,
            "session_token_masked": mask_token(session_token),
            "attack_type": "flooding",
            "target_to": target,
            "from_field": "flooder",
            "message_type": msg_type,
            "message_summary": f"flood_count={FLOOD_BATCH}",
            "raw_message": {"example": envelope},
            "outcome": {"batch_outcomes": outcomes},
            "response_summary": None
        }
        log_attack(rec)
        print("[attack][flooding] burst to", target, "count=", FLOOD_BATCH)

    async def attack_manipulation(self, session_id, session_token):
        # craft a TASK or PROPOSAL with altered payload fields
        target = self.choose_target_endpoint()
        original = {"task":"book_flight","params":{"from":"DEL","to":"BOM","date":"2025-12-10"}}
        # manipulation: flip destination and inject weird priority flag
        manipulated = dict(original)
        manipulated["params"] = dict(original["params"])
        manipulated["params"]["to"] = "XXX"  # invalid/ poisoned value
        manipulated["params"]["priority"] = "HIGH-PRIO-ATTACK"
        manipulated["payload_signature"] = "tampered"
        envelope = _make_env("TASK", f"manipulator-{random.randint(1,999)}", target, manipulated, session_id=session_id, session_token=session_token)
        res = await self.send_envelope(envelope)
        rec = {
            "timestamp_iso": now_iso(),
            "session_id": session_id,
            "session_token_masked": mask_token(session_token),
            "attack_type": "manipulation",
            "target_to": target,
            "from_field": envelope.get("from"),
            "message_type": "TASK",
            "message_summary": summarize_payload(manipulated),
            "raw_message": envelope,
            "outcome": res,
            "response_summary": None
        }
        log_attack(rec)
        print("[attack][manipulation] sent tampered TASK to", target)

    async def attack_state(self, session_id, session_token):
        # attempt to change session state by sending a fake state transition
        target = self.choose_target_endpoint()
        payload = {"action":"SESSION_UPDATE","state":"CONFUSED","notes":"attack_state_injection"}
        envelope = _make_env("SESSION_UPDATE", f"attacker-state-{random.randint(1,999)}", target, payload, session_id=session_id, session_token=session_token)
        res = await self.send_envelope(envelope)
        rec = {
            "timestamp_iso": now_iso(),
            "session_id": session_id,
            "session_token_masked": mask_token(session_token),
            "attack_type": "state",
            "target_to": target,
            "from_field": envelope.get("from"),
            "message_type": "SESSION_UPDATE",
            "message_summary": summarize_payload(payload),
            "raw_message": envelope,
            "outcome": res,
            "response_summary": None
        }
        log_attack(rec)
        print("[attack][state] attempted session-update to", target)

    async def attack_reply(self, session_id, session_token):
        # send a fake agent reply to orchestrator (PROPOSAL / AGGREGATED_RESULTS)
        target = "orchestrator"  # reply makes sense to orchestrator
        fake_agent = random.choice(["agent.flight","agent.hotel","agent.tourist"])
        payload = {"proposal_id": f"prop-{random.randint(1000,9999)}", "price": random.randint(100,999), "meta":{"attack":"spoof_reply"}}
        envelope = _make_env("PROPOSAL", fake_agent, target, payload, session_id=session_id, session_token=session_token)
        res = await self.send_envelope(envelope)
        rec = {
            "timestamp_iso": now_iso(),
            "session_id": session_id,
            "session_token_masked": mask_token(session_token),
            "attack_type": "reply",
            "target_to": target,
            "from_field": fake_agent,
            "message_type": "PROPOSAL",
            "message_summary": summarize_payload(payload),
            "raw_message": envelope,
            "outcome": res,
            "response_summary": None
        }
        log_attack(rec)
        print("[attack][reply] spoofed agent reply from", fake_agent)

    async def attack_fakelink(self, session_id, session_token):
        target = self.choose_target_endpoint()
        link = f"https://malicious.example.com/{random.randint(1000,9999)}?sid={session_id}"
        payload = {"title":"See this helpful link","url":link, "note":"fakelink"}
        envelope = _make_env("INCOMING_QUERY", f"linker-{random.randint(1,999)}", target, payload, session_id=session_id, session_token=session_token)
        res = await self.send_envelope(envelope)
        rec = {
            "timestamp_iso": now_iso(),
            "session_id": session_id,
            "session_token_masked": mask_token(session_token),
            "attack_type": "fakelink",
            "target_to": target,
            "from_field": envelope.get("from"),
            "message_type": envelope.get("type"),
            "message_summary": summarize_payload(payload),
            "raw_message": envelope,
            "outcome": res,
            "response_summary": None
        }
        log_attack(rec)
        print("[attack][fakelink] sent malicious link to", target)

    async def attack_replay(self, session_id, session_token):
        # replay recently seen message (if any)
        if not self.replay_store:
            print("[attack][replay] nothing to replay (store empty)")
            return
        sample = random.choice(self.replay_store)
        # try to resend sample but with same session_id to simulate replay
        # if the message was a dict shaped as {type,...} we can re-send as-is
        if isinstance(sample, dict):
            envelope = dict(sample)
            # ensure it contains session fields
            envelope["session_id"] = session_id
            envelope["session_token"] = session_token
            # update timestamp/message_id to mimic replay (or keep identical to simulate exact replay)
            # keep identical to simulate exact replay attack
            res = await self.send_envelope(envelope)
            rec = {
                "timestamp_iso": now_iso(),
                "session_id": session_id,
                "session_token_masked": mask_token(session_token),
                "attack_type": "replay",
                "target_to": envelope.get("to"),
                "from_field": envelope.get("from"),
                "message_type": envelope.get("type"),
                "message_summary": summarize_payload(envelope.get("payload")),
                "raw_message": envelope,
                "outcome": res,
                "response_summary": None
            }
            log_attack(rec)
            print("[attack][replay] replayed message to", envelope.get("to"))
        else:
            print("[attack][replay] stored sample not a dict, skipped")

# ---------------- CLI runner ----------------
async def main_run(sessions, runtime_per_session=None):
    engine = AttackEngine(GATEWAY_WS, sessions)
    await engine.connect()
    # create per-session tasks
    tasks = []
    for sid, stoken in sessions:
        tasks.append(asyncio.create_task(engine.run_attack_cycle(sid, stoken, runtime_seconds=runtime_per_session)))
    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        pass
    except Exception as e:
        print("main_run exception", e)
    finally:
        await engine.close()

if __name__ == "__main__":
    # parse simple args
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--gateway", default=GATEWAY_WS, help="Gateway websocket URL")
    parser.add_argument("--session", action="append", help="session as id:token (repeatable)")
    parser.add_argument("--runtime", type=int, default=None, help="seconds to run per session (optional)")
    args = parser.parse_args()
    if args.gateway:
        GATEWAY_WS = args.gateway
    if args.session:
        sessions = []
        for s in args.session:
            if ":" in s:
                sid, st = s.split(":",1)
                sessions.append((sid, st))
            else:
                print("session must be id:token")
        if not sessions:
            sessions = DEFAULT_SESSIONS
    else:
        sessions = DEFAULT_SESSIONS

    print("Attack engine starting. Gateway =", GATEWAY_WS, "sessions=", [s[0] for s in sessions])
    try:
        asyncio.run(main_run(sessions, runtime_per_session=args.runtime))
    except KeyboardInterrupt:
        print("Interrupted by user")
