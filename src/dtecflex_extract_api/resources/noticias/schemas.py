from datetime import datetime, date
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field


class NomeRaspadoSchema(BaseModel):
    ID: int
    NOME: Optional[str] = Field(None, max_length=250)
    CPF: Optional[str] = Field(None, max_length=14)
    APELIDO: Optional[str] = Field(None, max_length=100)
    NOME_CPF: Optional[str] = Field(None, max_length=270)
    SEXO: Optional[str] = Field(None, max_length=1,
                                description="M para masculino, F para feminino, N/A para não especificado")
    PESSOA: Optional[str] = Field(None, max_length=2)
    IDADE: Optional[int] = None
    ANIVERSARIO: Optional[date] = None
    ATIVIDADE: Optional[str] = Field(None, max_length=140)
    ENVOLVIMENTO: Optional[str] = Field(None, max_length=500)
    TIPO_SUSPEITA: Optional[str] = Field(None, max_length=20)
    FLG_PESSOA_PUBLICA: Optional[bool] = False
    INDICADOR_PPE: Optional[bool] = False
    OPERACAO: Optional[str] = Field(None, max_length=250)

    model_config = ConfigDict(from_attributes=True)


class NoticiaRaspadaBaseSchema(BaseModel):
    LINK_ID: str = Field(..., max_length=64)
    URL: Optional[str] = Field(..., max_length=1000)
    FONTE: str = Field(..., max_length=250)
    DATA_PUBLICACAO: Optional[datetime] = None
    CATEGORIA: str = Field(..., max_length=50)
    QUERY: Optional[str] = Field(None, max_length=250)
    ID_ORIGINAL: str = Field(..., max_length=2000)
    DT_RASPAGEM: Optional[datetime] = None
    DT_APROVACAO: Optional[datetime] = None
    DT_DECODE: Optional[datetime] = None
    TITULO: Optional[str] = Field(None, max_length=250)
    ID_USUARIO: Optional[int] = None
    STATUS: Optional[str] = Field(None, max_length=25)
    TEXTO_NOTICIA: Optional[str] = None
    LINK_ORIGINAL: Optional[str] = Field(None, max_length=2000)
    UF: Optional[str] = Field(None, max_length=200)
    REGIAO: Optional[str] = Field(None, max_length=200)

    model_config = ConfigDict(from_attributes=True)


class NoticiaRaspadaCreateSchema(NoticiaRaspadaBaseSchema):
    pass


class NoticiaRaspadaUpdateSchema(BaseModel):
    LINK_ID: Optional[str] = Field(None, max_length=64)
    URL: Optional[str] = Field(None, max_length=1000)
    FONTE: Optional[str] = Field(None, max_length=250)
    DATA_PUBLICACAO: Optional[datetime] = None
    CATEGORIA: Optional[str] = Field(None, max_length=50)
    QUERY: Optional[str] = Field(None, max_length=250)
    ID_ORIGINAL: Optional[str] = Field(None, max_length=2000)
    DT_RASPAGEM: Optional[datetime] = None
    DT_APROVACAO: Optional[datetime] = None
    DT_DECODE: Optional[datetime] = None
    TITULO: Optional[str] = Field(None, max_length=250)
    ID_USUARIO: Optional[int] = None
    STATUS: Optional[str] = Field(None, max_length=25)
    TEXTO_NOTICIA: Optional[str] = None
    LINK_ORIGINAL: Optional[str] = Field(None, max_length=2000)
    REG_NOTICIA: Optional[str] = Field(None, max_length=20)
    UF: Optional[str] = Field(None, max_length=2)
    REGIAO: Optional[str] = Field(None, max_length=200)
    model_config = ConfigDict(from_attributes=True)


class NoticiaRaspadaSchema(NoticiaRaspadaBaseSchema):
    ID: int
    LINK_ID: Optional[str] = Field(None, max_length=64)
    URL: Optional[str] = Field(None, max_length=1000)
    FONTE: Optional[str] = Field(None, max_length=250)
    DATA_PUBLICACAO: Optional[datetime] = None
    CATEGORIA: Optional[str] = Field(None, max_length=50)
    QUERY: Optional[str] = Field(None, max_length=250)
    ID_ORIGINAL: Optional[str] = Field(None, max_length=2000)
    DT_APROVACAO: Optional[datetime] = None
    DT_RASPAGEM: Optional[datetime] = None
    DT_DECODE: Optional[datetime] = None
    TITULO: Optional[str] = Field(None, max_length=250)
    ID_USUARIO: Optional[int] = None
    STATUS: Optional[str] = Field(None, max_length=25)
    TEXTO_NOTICIA: Optional[str] = None
    LINK_ORIGINAL: Optional[str] = Field(None, max_length=2000)
    REG_NOTICIA: Optional[str] = Field(None, max_length=20)
    UF: Optional[str] = Field(None, max_length=200)
    REGIAO: Optional[str] = Field(None, max_length=200)
    mensagens: Optional[list] = None
    nomes_raspados: Optional[list[NomeRaspadoSchema]] = None

    model_config = ConfigDict(from_attributes=True)
