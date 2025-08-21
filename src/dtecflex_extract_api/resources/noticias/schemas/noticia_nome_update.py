# --- schemas.py (ou junto das rotas) ---
from pydantic import BaseModel, Field
from typing import Optional, List
from datetime import date

class NoticiaNomePartialUpdate(BaseModel):
    id: int  # obrigatório para update
    nome: Optional[str] = None
    cpf: Optional[str] = None
    apelido: Optional[str] = None
    nome_cpf: Optional[str] = None
    operacao: Optional[str] = None
    sexo: Optional[str] = None
    pessoa: Optional[str] = None
    idade: Optional[int] = None
    atividade: Optional[str] = None
    envolvimento: Optional[str] = None
    tipo_suspeita: Optional[str] = None
    flg_pessoa_publica: Optional[bool] = None
    indicador_ppe: Optional[bool] = None
    aniversario: Optional[date] = None

class NoticiaNomesBatchUpdateIn(BaseModel):
    nomes: List[NoticiaNomePartialUpdate] = Field(..., min_items=1)
