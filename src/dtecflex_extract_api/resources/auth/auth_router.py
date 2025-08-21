from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordRequestForm
import jwt
from datetime import datetime, timedelta

from src.dtecflex_extract_api.config.auth import SECRET_KEY, ALGORITHM, ACCESS_TOKEN_EXPIRE_MINUTES, DEFAULT_EXPIRE_MINUTES
from src.dtecflex_extract_api.config.database import get_auth_service
from src.dtecflex_extract_api.resources.auth.auth_service import AuthService
from src.dtecflex_extract_api.resources.usuario.entities.usuario import UsuarioModel
from src.dtecflex_extract_api.shared.utils.get_current_user import get_current_user

router = APIRouter()

def create_access_token(
    data: dict,
    expires_delta: Optional[timedelta] = timedelta(minutes=DEFAULT_EXPIRE_MINUTES)
) -> str:
    to_encode = data.copy()
    if expires_delta is not None:
        expire = datetime.utcnow() + expires_delta
        to_encode["exp"] = expire
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)

@router.post("/login")
async def login_for_access_token(
    form_data: OAuth2PasswordRequestForm = Depends(),
    auth_service: AuthService = Depends(get_auth_service)
):
    user = auth_service.buscar_usuario(form_data.username)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Usuário ou senha incorretos",
        )
    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(
        data={"sub": user.USERNAME}, expires_delta=access_token_expires)

    return {"access_token": access_token, "token_type": "bearer"}

@router.get("/me")
async def read_users_me(
    current_user: UsuarioModel = Depends(get_current_user)
):
    response = {
        "USERNAME": current_user.USERNAME,
        "ID": current_user.ID
    }

    return response