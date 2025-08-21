from fastapi import Depends, status, HTTPException
from jose import jwt, JWTError

from src.dtecflex_extract_api.config.auth import oauth2_scheme, SECRET_KEY, ALGORITHM
from src.dtecflex_extract_api.config.database import get_auth_service
from src.dtecflex_extract_api.resources.auth.auth_service import AuthService
from src.dtecflex_extract_api.resources.usuario.entities.usuario import UsuarioModel


async def get_current_user(
    token: str = Depends(oauth2_scheme),
    auth_service: AuthService = Depends(get_auth_service)
) -> UsuarioModel:
    credential_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise credential_exception
    except JWTError:
        raise credential_exception

    user = auth_service.buscar_usuario(username)
    if user is None:
        raise credential_exception
    return user