from fastapi import Depends, HTTPException, status
from sqlalchemy.orm import Session
from jose import JWTError, jwt
from src.dtecflex_extract_api.config.auth import oauth2_scheme, SECRET_KEY, ALGORITHM, TokenData
from src.dtecflex_extract_api.resources.usuario.entities.usuario import UsuarioModel


class AuthService:

    def __init__(self, session: Session):
        self.session = session

    def buscar_usuario(self, nome):
        try:
            return self.session.query(UsuarioModel).filter_by(USERNAME=nome).first()
        except Exception as e:
#            if not self._verify_password()
            raise Exception("Erro ao buscar usuário.")

#    def _verify_password(plain_password, hashed_password):
#        return pwd_context.verify(plain_password, hashed_password)
    async def get_current_user_by_token(
        self,
        token: str = Depends(oauth2_scheme)
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
            token_data = TokenData(username)
        except JWTError:
            raise credential_exception

        user = await self.buscar_usuario(token_data.username)  # ou sync, conforme seu método
        if user is None:
            raise credential_exception
        return user