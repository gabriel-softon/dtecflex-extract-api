from fastapi import APIRouter, WebSocket, WebSocketDisconnect, Query
from src.dtecflex_extract_api.services.transfer_service import normalize_category
from src.dtecflex_extract_api.utils.pubsub import r_async, channel_name, job_key, get_meta

router = APIRouter()

@router.websocket("/ws/publication")
async def publication_ws(
    websocket: WebSocket,
    date: str = Query(..., pattern=r"^\d{8}$"),       # YYYYMMDD
    category: str | None = Query(None)                # CR/LD/FF/SE/SA ou por extenso
):
    await websocket.accept()
    abrev, cat_prefix, _ = normalize_category(category) if category else (None, None, None)
    key = job_key(date, abrev, cat_prefix)
    chan = channel_name(key)

    # envia estado inicial (se houver)
    meta = get_meta(key)
    if meta:
        await websocket.send_json({"event": "SNAPSHOT", **meta})

    # assina o canal e repassa mensagens
    pubsub = r_async.pubsub()
    await pubsub.subscribe(chan)
    try:
        async for msg in pubsub.listen():
            if msg and msg.get("type") == "message":
                await websocket.send_text(msg["data"])
    except WebSocketDisconnect:
        pass
    finally:
        try:
            await pubsub.unsubscribe(chan)
        except Exception:
            pass
        await pubsub.aclose()
