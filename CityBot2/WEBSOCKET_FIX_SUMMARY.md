# WebSocket Connection Fix Summary

## Problem Identified

The error you were experiencing:
```
RuntimeError (disconnect race) from [client]: Cannot call "receive" once a disconnect message has been received.
```

This was a **race condition** between the server's background Redis subscriber and the client's receive loop. The issue occurred when:

1. **Gateway** tries to send a message from Redis to a client (via `redis_subscriber()`)
2. **Client** simultaneously receives a disconnect signal
3. **Gateway** then tries to call `receive()` on the already-disconnected WebSocket
4. This causes a RuntimeError because the socket state changed between operations

## Root Causes Fixed

### 1. **Gateway (gateway.py)**
- **Problem**: No synchronization between concurrent sends and receives on the same WebSocket
- **Solution**: 
  - Added `ClientConnection` wrapper class with `asyncio.Lock` for send operations
  - Ensures only one send operation happens at a time
  - Tracks connection state to prevent sends to disconnected clients
  - Added `send_to_client_safe()` function with proper exception handling

### 2. **All Clients (flight_agent, hotel_agent, tourist_agent, orchestrator, user_sim)**
- **Problem**: Race condition between gateway sending and client receiving
- **Solution**:
  - Added `close_timeout=5` to WebSocket connections for cleaner shutdowns
  - Improved CONNECT/CONNECTED handshake with explicit acknowledgment wait
  - Added timeout to receive operations to detect stalled connections
  - Better separation of connection setup from message loop
  - Clearer exception handling and logging

## Key Changes

### Gateway Changes
```python
# NEW: Thread-safe connection wrapper
class ClientConnection:
    def __init__(self, websocket: WebSocket):
        self.websocket = websocket
        self.client_type: str | None = None
        self.connected = True
        self.send_lock = asyncio.Lock()  # Prevents concurrent sends

# NEW: Safe send function with proper locking
async def send_to_client_safe(client_conn: ClientConnection, envelope: dict) -> bool:
    if not client_conn.connected:
        return False
    
    async with client_conn.send_lock:  # Only one send at a time
        # Protected send operation with timeout
```

### Client Connection Improvements

**Connection Setup Pattern (all agents)**:
```python
async with websockets.connect(
    GATEWAY_WS,
    ping_interval=20,      # Keep-alive ping every 20s
    ping_timeout=10,       # Timeout for ping response
    close_timeout=5        # NEW: Graceful close timeout
) as ws:
    # Send CONNECT
    await ws.send(...)
    
    # Wait for CONNECTED ack (prevents race)
    raw = await asyncio.wait_for(ws.recv(), timeout=5.0)
    ack = json.loads(raw)
    
    # Main receive loop with timeout
    while True:
        raw = await asyncio.wait_for(ws.recv(), timeout=60.0)
```

## Testing the Fix

### Start Gateway
```powershell
# Ensure Redis is running
docker run -p 6379:6379 -d redis:alpine

# Start gateway
cd c:\Users\bhatt\Vansh\EDAI_SEM5\CityBot2
uvicorn gateway:app --host 0.0.0.0 --port 8000
```

### Start All Components (in separate terminals)
```powershell
# Terminal 1: Flight Agent
python agents/flight_agent.py

# Terminal 2: Hotel Agent
python agents/hotel_agent.py

# Terminal 3: Tourist Agent
python agents/tourist_agent.py

# Terminal 4: Orchestrator
python orchestrator.py

# Terminal 5: User Simulator
python user_sim.py
```

### Expected Output
```
INFO     Started server process
INFO     Gateway startup complete
INFO     WebSocket connected: flight_agent
INFO     Received CONNECTED ack from gateway
INFO     WebSocket connected: hotel_agent
...
INFO     User received AGGREGATED_RESULTS
```

**Key difference**: You should NOT see the `RuntimeError (disconnect race)` messages anymore.

## Architecture Improvements

1. **Explicit Synchronization**: WebSocket sends are now protected by locks
2. **State Tracking**: Connection state is explicitly managed and checked before operations
3. **Timeout Protection**: All receive operations have explicit timeouts
4. **Proper Handshake**: CONNECT/CONNECTED handshake ensures both sides are ready
5. **Clean Resource Cleanup**: Proper connection state flags prevent stale operations

## Why This Works

1. **Lock on gateway side**: Prevents multiple concurrent sends that could interfere with client receives
2. **Connection state flag**: Prevents operations on dead connections
3. **Explicit timeout**: Detects network issues early
4. **Proper handshake**: Ensures server and client are synchronized before main loop
5. **Keep-alive pings**: Detect dead connections automatically (ping_interval=20s)

## Monitoring

Watch for these log patterns:
- ✅ `Received CONNECTED ack from gateway` - Connection successful
- ✅ `WebSocket connected: [client_id]` - Server accepted connection
- ✅ `Reconnecting in Xs...` - Normal backoff, not an error
- ❌ `RuntimeError (disconnect race)` - Should NOT appear anymore
- ❌ `Cannot call "receive" once a disconnect` - Should NOT appear anymore

## Notes

- Redis connection to `redis://localhost:6379` is required
- All components support automatic reconnection with exponential backoff
- Maximum reconnection backoff is 30 seconds
- Session tokens have a default TTL of 900 seconds
