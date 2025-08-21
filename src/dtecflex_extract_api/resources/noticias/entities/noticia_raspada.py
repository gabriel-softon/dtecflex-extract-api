from sqlalchemy import Column, DateTime, Integer, String, Text, ForeignKey, func, BigInteger, Date
from sqlalchemy.orm import relationship

from src.dtecflex_extract_api.config.base import Base

class NoticiaRaspadaModel(Base):
    __tablename__ = 'TB_NOTICIA_RASPADA'

    ID = Column(Integer, primary_key=True, index=True)
    LINK_ID = Column(String(64), nullable=False, unique=True)
    URL = Column(String(1000), nullable=False)
    FONTE = Column(String(250), nullable=False)
    DATA_PUBLICACAO = Column(DateTime, nullable=True)
    REGIAO = Column(String(250), nullable=True)
    UF = Column(String(2), nullable=True)
    # DATA_ANALISE = Column(DateTime, nullable=True)
    CATEGORIA = Column(String(50), nullable=False)
    REG_NOTICIA = Column(String(20), nullable=True)
    QUERY = Column(String(250), nullable=True)
    ID_ORIGINAL = Column(String(2000), nullable=False)
    DT_RASPAGEM = Column(DateTime, nullable=False, server_default=func.now())
    DT_APROVACAO = Column(DateTime, nullable=False, server_default=func.now())
    DT_TRANSFERENCIA = Column(DateTime, nullable=False, server_default=func.now())
    DT_DECODE = Column(DateTime, nullable=True)
    TITULO = Column(String(250), nullable=True)
    ID_USUARIO = Column(Integer, ForeignKey('TB_USER.ID'), nullable=True)
    STATUS = Column(String(25), nullable=True)
    TEXTO_NOTICIA = Column(Text, nullable=True)
    LINK_ORIGINAL = Column(String(2000), nullable=True)

    usuario = relationship("UsuarioModel", back_populates="noticias_raspadas")
    nomes_raspados = relationship("NoticiaRaspadaNomeModel", back_populates="noticia")
    # mensagens = relationship("NoticiaRaspadaMsgModel", back_populates="noticia")


    def __repr__(self):
        return f"<NoticiaRaspadaModel(ID={self.ID}, TITULO='{self.TITULO}')>"

class NoticiaRaspadaNomeModel(Base):
    __tablename__ = 'TB_NOTICIA_RASPADA_NOME'

    ID = Column(BigInteger, primary_key=True, index=True, autoincrement=True)
    NOTICIA_ID = Column(
        Integer,
        ForeignKey('TB_NOTICIA_RASPADA.ID', ondelete="NO ACTION", onupdate="NO ACTION"),
        nullable=False,
        index=True
    )
    NOME = Column(String(100, collation='utf8mb4_unicode_ci'), nullable=False)
    CPF = Column(String(14, collation='utf8mb4_unicode_ci'), nullable=True, server_default='0')
    NOME_CPF = Column(String(100, collation='utf8mb4_unicode_ci'), nullable=True)
    APELIDO = Column(String(50, collation='utf8mb4_unicode_ci'), nullable=True)
    OPERACAO = Column(String(50, collation='utf8mb4_unicode_ci'), nullable=True)
    # REG_NOTICIA = Column(String(20, collation='utf8mb4_unicode_ci'), nullable=True)
    SEXO = Column(String(1, collation='utf8mb4_unicode_ci'), nullable=True)
    PESSOA = Column(String(2, collation='utf8mb4_unicode_ci'), nullable=True)
    IDADE = Column(Integer, nullable=True)
    ATIVIDADE = Column(String(140, collation='latin1_general_ci'), nullable=True)
    ENVOLVIMENTO = Column(String(500, collation='utf8mb4_unicode_ci'), nullable=True)
    TIPO_SUSPEITA = Column(String(20, collation='utf8mb4_unicode_ci'), nullable=True)
    FLG_PESSOA_PUBLICA = Column(String(1, collation='utf8mb4_unicode_ci'), nullable=True)
    ANIVERSARIO = Column(Date, nullable=True)
    INDICADOR_PPE = Column(String(1, collation='utf8mb4_unicode_ci'), nullable=True)

    noticia = relationship("NoticiaRaspadaModel", back_populates="nomes_raspados")

    def __repr__(self):
        return f"<NoticiaRaspadaNomeModel(ID={self.ID}, NOME='{self.NOME}')>"
