# messages.py
from typing import Any, Dict
from uuid import uuid4
import time
import json

def now_ts() -> int:
    return int(time.time() * 1000)

def make_envelope(type_: str, from_: str, to: str, payload: Dict[str, Any], session_id: str = None, session_token: str = None):
    return {
        "type": type_,
        "session_id": session_id,
        "session_token": session_token,
        "from": from_,
        "to": to,
        "message_id": str(uuid4()),
        "timestamp": now_ts(),
        "payload": payload
    }

def dumps(msg: dict) -> str:
    return json.dumps(msg)
