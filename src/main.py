# main.py
import json
import asyncio
from contextlib import asynccontextmanager
from fastapi.security import OAuth2PasswordBearer

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
import redis.asyncio as aioredis

from src.dtecflex_extract_api.resources.noticias.noticias_router import router as noticias_router
from src.dtecflex_extract_api.resources.auth.auth_router import router as auth_router
from fastapi.middleware.cors import CORSMiddleware

# se teu Angular roda em http://localhost:4200
origins = [
    "*",
    # se for necessário:
    # "http://127.0.0.1:4200",
    # "http://seu-dominio-ou-ip"
]
# gerencia conexões WebSocket
class ConnectionManager:
    def __init__(self):
        self.active: set[WebSocket] = set()

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self.active.add(ws)

    def disconnect(self, ws: WebSocket):
        self.active.discard(ws)

    async def broadcast(self, message: str):
        for ws in list(self.active):
            try:
                await ws.send_text(message)
            except WebSocketDisconnect:
                self.disconnect(ws)

manager = ConnectionManager()

# @asynccontextmanager
# async def lifespan(app: FastAPI):
#     app.state.redis = aioredis.from_url(
#         "redis://localhost:6379/2",
#         decode_responses=True,
#     )
#     pubsub = app.state.redis.pubsub()
#     await pubsub.subscribe("cpf_status")

#     asyncio.create_task(_pubsub_listener(pubsub))

#     yield

#     await pubsub.unsubscribe("cpf_status")
#     await pubsub.close()
#     await app.state.redis.close()

app = FastAPI(
    title="Relações PEP API",
    version="1.0",
    # lifespan=lifespan
    )
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.include_router(noticias_router, prefix="/noticias", tags=["Notícias"])
app.include_router(auth_router, prefix="/auth", tags=["Auth"])

# @app.websocket("/ws")
# async def websocket_endpoint(ws: WebSocket):
#     await manager.connect(ws)
#     try:
#         while True:
#             await ws.receive_text()
#     except WebSocketDisconnect:
#         manager.disconnect(ws)

# async def _pubsub_listener(pubsub):
#     while True:
#         msg = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
#         if msg and msg["type"] == "message":
#             # msg["data"] já é string JSON, basta broadcastar
#             await manager.broadcast(msg["data"])
#         await asyncio.sleep(0.01)
