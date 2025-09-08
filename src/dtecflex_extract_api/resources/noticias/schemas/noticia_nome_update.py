# --- schemas.py (ou junto das rotas) ---
from pydantic import BaseModel, Field, field_validator
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

    @field_validator(
        'cpf','apelido','nome_cpf','operacao','sexo','pessoa',
        'atividade','envolvimento','tipo_suspeita', mode='before'
    )
    @classmethod
    def blank_to_none(cls, v):
        if isinstance(v, str) and v.strip() == '':
            return None
        return v

    @field_validator('aniversario', mode='before')
    @classmethod
    def aniversario_blank_to_none(cls, v):
        return None if v in ('', None) else v

class NoticiaNomesBatchUpdateIn(BaseModel):
    nomes: List[NoticiaNomePartialUpdate] = Field(..., min_items=1)
