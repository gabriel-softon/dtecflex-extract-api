from datetime import datetime
from typing import Optional
from pydantic import BaseModel, Field

class NoticiaCreate(BaseModel):
    url: str = Field(..., max_length=1000)
    fonte: str = Field(..., max_length=250)
    categoria: str = Field(..., max_length=50)

    data_publicacao: Optional[datetime] = None
    regiao: Optional[str] = Field(None, max_length=250)
    uf: Optional[str] = Field(None, max_length=2)
    reg_noticia: Optional[str] = Field(None, max_length=20)
    query: Optional[str] = Field(None, max_length=250)
    titulo: Optional[str] = Field(None, max_length=250)
    status: Optional[str] = Field(None, max_length=25)
    texto_noticia: Optional[str] = None
    link_original: Optional[str] = Field(None, max_length=2000)