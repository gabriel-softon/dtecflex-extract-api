from sqlalchemy import Column, Integer, String, Boolean
from sqlalchemy.orm import relationship

from src.dtecflex_extract_api.config.base import Base


class UsuarioModel(Base):
    __tablename__ = 'TB_USER'

    ID = Column(Integer, primary_key=True, autoincrement=True)
    # NOME = Column(String(50), nullable=False)
    USERNAME = Column(String(120), unique=True, nullable=False)
    SENHA = Column(String(129), nullable=False)
    ADMIN = Column(Boolean, default=False, nullable=False)

    noticias_raspadas = relationship("NoticiaRaspadaModel", back_populates="usuario")
#    mensagens = relationship("NoticiaRaspadaMsgModel", back_populates="usuario")

    # def __repr__(self):
    #     return f"<UsuarioModel(ID={self.ID}, USERNAME='{self.USERNAME}', ADMIN={self.ADMIN})>"
