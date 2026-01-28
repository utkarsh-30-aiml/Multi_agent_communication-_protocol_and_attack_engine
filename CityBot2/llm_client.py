# llm_client.py
# Simple wrapper around Ollama client library. Uses 'generate' style API.
from typing import Dict, Any
import asyncio

# Ollama docs: see Ollama Python library. :contentReference[oaicite:7]{index=7}
try:
    from ollama import generate
except Exception:
    # if import fails, raise clearer error
    raise RuntimeError("ollama package not installed. pip install ollama and ensure local ollama daemon is running.")

async def ask_ollama(model: str, prompt: str, max_tokens: int = 512) -> str:
    """
    Wraps ollama generate call. Ollama's python client is synchronous,
    but we call it in a thread to avoid blocking the asyncio loop.
    """
    loop = asyncio.get_event_loop()
    def _call():
        # Example usage from docs: generate(model, prompt)
        resp = generate(model, prompt)
        # resp structure may contain keys like 'response' or 'text' depending on version
        if isinstance(resp, dict):
            # try common keys
            for k in ("response", "text", "output"):
                if k in resp:
                    return resp[k]
            # fallback to str
            return str(resp)
        return str(resp)

    return await loop.run_in_executor(None, _call)
