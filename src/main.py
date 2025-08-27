from fastapi import FastAPI, APIRouter
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import OAuth2PasswordBearer

from src.dtecflex_extract_api.resources.noticias.noticias_router import router as noticias_router
from src.dtecflex_extract_api.resources.auth.auth_router import router as auth_router

app = FastAPI(
    title="Relações PEP API",
    version="1.0",
    docs_url="/api/docs",
    openapi_url="/api/openapi.json",
)

app.add_middleware(CORSMiddleware,
    allow_origins=["*"], allow_credentials=True,
    allow_methods=["*"], allow_headers=["*"],
)

# 👉 aponta para o endpoint que realmente existe:
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/auth/login")

api = APIRouter(prefix="/api")
api.include_router(noticias_router, prefix="/noticias", tags=["Notícias"])
api.include_router(auth_router,     prefix="/auth",     tags=["Auth"])
app.include_router(api)
