import datetime
import asyncio
from typing import Dict, Set
from fastapi import APIRouter, WebSocket, WebSocketDisconnect, Depends
from sqlalchemy.orm import Session

from app.core.config import settings
from app.db.database import get_db

router = APIRouter()


class RegimeStreamManager:
    def __init__(self):
        self.connections: Dict[str, Set[WebSocket]] = {}
        self._lock = asyncio.Lock()

    async def connect(self, websocket: WebSocket, coin: str):
        await websocket.accept()
        async with self._lock:
            if coin not in self.connections:
                self.connections[coin] = set()
            self.connections[coin].add(websocket)

    async def disconnect(self, websocket: WebSocket, coin: str):
        async with self._lock:
            if coin in self.connections:
                self.connections[coin].discard(websocket)

    async def broadcast_regime_update(self, coin: str, data: dict):
        if coin not in self.connections:
            return
        dead_connections = set()
        for websocket in self.connections[coin].copy():
            try:
                await websocket.send_json(data)
            except Exception:
                dead_connections.add(websocket)
        async with self._lock:
            self.connections[coin] -= dead_connections

    def connection_count(self, coin: str = None) -> int:
        if coin:
            return len(self.connections.get(coin, set()))
        return sum(len(v) for v in self.connections.values())


stream_manager = RegimeStreamManager()


@router.websocket("/ws/regime/{coin}")
async def regime_websocket(
    websocket: WebSocket,
    coin: str,
    token: str = None,
    db: Session = Depends(get_db),
):
    """
    WebSocket endpoint for live regime updates.
    Connect with: ws://your-backend/ws/regime/BTC?token=YOUR_TOKEN

    Messages received:
      - "ping" -> server replies "pong"

    Messages sent:
      - regime_snapshot: current state on connect
      - regime_update: pushed when regime changes
      - heartbeat: every 30s to keep connection alive
    """
    # Auth check
    if not token:
        await websocket.close(code=4001, reason="Authentication required")
        return

    from app.auth.auth import resolve_user_tier
    user_info = resolve_user_tier(f"Bearer {token}", db)
    if not user_info["is_pro"]:
        await websocket.close(code=4003, reason="Pro subscription required")
        return

    coin = coin.upper()
    if coin not in settings.SUPPORTED_COINS:
        await websocket.close(code=4004, reason="Unsupported coin")
        return

    await stream_manager.connect(websocket, coin)

    try:
        # Send current state immediately on connect
        from app.services.market_data import build_regime_stack
        stack = build_regime_stack(coin, db)
        await websocket.send_json({
            "type": "regime_snapshot",
            "coin": coin,
            "data": stack,
            "timestamp": datetime.datetime.utcnow().isoformat(),
            "connections": stream_manager.connection_count(coin),
        })

        # Keep connection alive
        while True:
            try:
                data = await asyncio.wait_for(
                    websocket.receive_text(),
                    timeout=30,
                )
                if data == "ping":
                    await websocket.send_text("pong")
            except asyncio.TimeoutError:
                # Send heartbeat every 30s
                await websocket.send_json({
                    "type": "heartbeat",
                    "timestamp": datetime.datetime.utcnow().isoformat(),
                })

    except WebSocketDisconnect:
        pass
    except Exception:
        pass
    finally:
        await stream_manager.disconnect(websocket, coin)


@router.get("/ws/stats")
def websocket_stats():
    """Shows how many clients are connected per coin."""
    return {
        "total_connections": stream_manager.connection_count(),
        "by_coin": {
            coin: stream_manager.connection_count(coin)
            for coin in settings.SUPPORTED_COINS
        },
    }


async def push_regime_update(coin: str, stack: dict):
    """
    Call this from update_market() to push live updates
    to all connected WebSocket clients for that coin.
    """
    await stream_manager.broadcast_regime_update(coin, {
        "type": "regime_update",
        "coin": coin,
        "data": stack,
        "timestamp": datetime.datetime.utcnow().isoformat(),
    })
