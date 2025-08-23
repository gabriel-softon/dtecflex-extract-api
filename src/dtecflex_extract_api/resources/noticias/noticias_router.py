from collections import defaultdict
import requests
from celery.result import AsyncResult
from fastapi import HTTPException, status, Request
from pydantic import BaseModel, Field

from src.dtecflex_extract_api.config.celery import celery_app
from src.dtecflex_extract_api.resources.noticias.entities.noticia_raspada import NoticiaRaspadaModel
from src.dtecflex_extract_api.resources.noticias.schemas.noticia_create import NoticiaCreate
from src.dtecflex_extract_api.resources.noticias.schemas.noticia_nome_update import NoticiaNomesBatchUpdateIn
from src.dtecflex_extract_api.resources.usuario.entities.usuario import UsuarioModel
from src.dtecflex_extract_api.shared.utils.get_current_user import get_current_user
import xml.etree.ElementTree as ET
from datetime import datetime, date, time
from typing import Optional, List, Dict, Any
from fastapi import APIRouter, Depends, Query
from src.dtecflex_extract_api.config.database import get_noticia_service
from src.dtecflex_extract_api.resources.noticias.noticias_service import NoticiaService
from src.dtecflex_extract_api.tasks.test import ping, add
from src.dtecflex_extract_api.tasks.transfer import transfer_task

router = APIRouter()

class NoticiaResponse(BaseModel):
    id: int
    link_id: str
    url: str
    fonte: str
    categoria: str
    titulo: Optional[str] = None
    status: Optional[str] = None
    data_publicacao: Optional[datetime] = None
    dt_raspagem: datetime

class AprovarNoticiasIn(BaseModel):
    ids: List[int] = Field(..., min_items=1, description="IDs das notícias a aprovar")

class NoticiaRequest(BaseModel):
    url: str

class NoticiaUpdateSchema(BaseModel):
    fonte:         Optional[str] = None
    titulo:        Optional[str] = None
    categoria:     Optional[str] = None
    regiao:        Optional[str] = None
    uf:            Optional[str] = None
    reg_noticia:   Optional[str] = None
    texto_noticia: Optional[str] = None
    status:        Optional[str] = None

class NoticiaNomeCreate(BaseModel):
    noticia_id: int = Field(..., alias="noticia_id")
    nome: str
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
    indicador_ppe:       Optional[bool] = None
    # envolvimento_gov:    Optional[bool] = None

    aniversario: Optional[date] = None

class NoticiaNomeResponse(NoticiaNomeCreate):
    id: int

    class Config:
        orm_mode = True
        allow_population_by_field_name = True

@router.post("/nome", response_model=NoticiaNomeResponse)
def create_noticia_nome(
    payload: NoticiaNomeCreate,
    noticia_service: NoticiaService = Depends(get_noticia_service)
):
    try:
        new = noticia_service.create_nome(payload)

        return {
            "id":                   new.ID,
            "noticia_id":           new.NOTICIA_ID,
            "nome":                 new.NOME,
            "cpf":                  new.CPF,
            "apelido":              new.APELIDO,
            "nome_cpf":             new.NOME_CPF,
            "operacao":             new.OPERACAO,
            "sexo":                 new.SEXO,
            "pessoa":               new.PESSOA,
            "idade":                new.IDADE,
            "atividade":            new.ATIVIDADE,
            "envolvimento":         new.ENVOLVIMENTO,
            "tipo_suspeita":        new.TIPO_SUSPEITA,
            "flg_pessoa_publica":   True  if new.FLG_PESSOA_PUBLICA   == "1" else False,
            "aniversario":          new.ANIVERSARIO,
            "indicador_ppe":        True  if new.INDICADOR_PPE        == "1" else False,
            # "envolvimento_gov":     True  if new.ENVOLVIMENTO_GOV     == "1" else False,
        }
    except Exception as e:
        print('err', e)
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/nome/{nome_id}", status_code=204)
def delete_noticia_nome(
    nome_id: int,
    noticia_service: NoticiaService = Depends(get_noticia_service)
):
    try:
        noticia_service.delete_nome(nome_id)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("")
def list_noticias(
    request: Request,
    page: int = Query(1, ge=1),
    limit: int = Query(10, ge=1),
    fonte: Optional[str] = Query(None, alias="fonte"),
    categoria: Optional[str] = Query(None, alias="categoria"),
    status: Optional[List[str]] = Query(None, alias="status"),
    data_inicio: Optional[str] = Query(None, alias="data_inicio"),
    dt_aprovacao: Optional[str] = Query(None, alias="dt_aprovacao"),
    data_fim: Optional[str] = Query(None, alias="data_fim"),
    usuario_id: Optional[int] = Query(None, alias="usuario_id"),
    noticia_service: NoticiaService = Depends(get_noticia_service),
    current_user: UsuarioModel = Depends(get_current_user),
):
    def parse_date(d: Optional[str]) -> Optional[datetime]:
        return datetime.strptime(d, "%Y-%m-%d") if d else None

    di = parse_date(data_inicio)
    df = parse_date(data_fim)
    if df:
        df = df.replace(hour=23, minute=59, second=59, microsecond=999999)

    start_aprv = end_aprv = None
    if dt_aprovacao:
        d = datetime.strptime(dt_aprovacao, "%Y-%m-%d").date()
        start_aprv = datetime.combine(d, time.min)
        end_aprv = datetime.combine(d, time.max)

    if status and len(status) == 1 and "," in status[0]:
        status = [s.strip() for s in status[0].split(",") if s.strip()]

    offset = (page - 1) * limit

    filters: Dict[str, Any] = {}
    if fonte:
        filters["FONTE"] = fonte
    if categoria:
        filters["CATEGORIA"] = categoria
    if status:
        filters["STATUS"] = status
    if usuario_id:
        filters["USUARIO_ID"] = usuario_id
    if start_aprv or end_aprv:
        filters["DT_APROVACAO"] = (start_aprv, end_aprv)

    if di and df:
        filters["DATA_PUBLICACAO"] = (di, df)
    elif di:
        filters["DATA_PUBLICACAO"] = (di, None)
    elif df:
        filters["DATA_PUBLICACAO"] = (None, df)

    noticias, total_count = noticia_service.list(offset=offset, limit=limit, filters=filters)

    total_pages = (total_count + limit - 1) // limit

    next_url = (
        str(request.url.include_query_params(page=page + 1, limit=limit))
        if page < total_pages else None
    )
    prev_url = (
        str(request.url.include_query_params(page=page - 1, limit=limit))
        if page > 1 else None
    )

    return {
        "total_count": total_count,
        "total_pages": total_pages,
        "page": page,
        "next": next_url,
        "previous": prev_url,
        "noticias": noticias,
    }

@router.post("", response_model=NoticiaResponse, status_code=status.HTTP_201_CREATED)
def create_noticia(
    payload: NoticiaCreate,
    noticia_service: NoticiaService = Depends(get_noticia_service)
):
    try:
        new = noticia_service.create(payload)
        return NoticiaResponse(
            id=new.ID,
            link_id=new.LINK_ID,
            url=new.URL,
            fonte=new.FONTE,
            categoria=new.CATEGORIA,
            titulo=new.TITULO,
            status=new.STATUS,
            data_publicacao=new.DATA_PUBLICACAO,
            dt_raspagem=new.DT_RASPAGEM,
        )
#    except ValueError as e:
#        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))

@router.get("/buscar-dtec", response_model=List[Dict[str, str]])
def buscar_dtec(
    nome: str = Query(..., min_length=1, description="Nome a pesquisar no DTEC"),
    rows: int = Query(20, ge=1, le=100, description="Quantidade de resultados")
    ,
    noticia_service: NoticiaService = Depends(get_noticia_service),
):
    try:
        return noticia_service.buscar_no_dtec(nome, rows)
    except ValueError as e:
        # credenciais ausentes/misconfiguradas
        raise HTTPException(status_code=500, detail=str(e))
    except requests.Timeout:
        raise HTTPException(status_code=504, detail="Timeout ao consultar DTEC.")
    except requests.HTTPError as e:
        code = e.response.status_code if e.response is not None else 502
        raise HTTPException(status_code=code, detail="Erro HTTP do serviço DTEC.")
    except (requests.RequestException, ET.ParseError):
        raise HTTPException(status_code=502, detail="Falha ao consultar ou interpretar resposta do DTEC.")

@router.post("/aprovar", summary="Aprovar notícias em lote")
def aprovar_noticias(
    payload: AprovarNoticiasIn,
    noticia_service: NoticiaService = Depends(get_noticia_service),
    current_user: UsuarioModel = Depends(get_current_user)
) -> Dict[str, Any]:
    """
    Recebe uma lista de IDs e aprova todas as notícias correspondentes.
    Retorna quantas foram atualizadas, quais IDs foram atualizados e quais não foram encontrados.
    """
    try:
        result = noticia_service.aprovar_em_lote(payload.ids)
        return result
    except Exception as e:
        # registre/log o erro se desejar
        raise HTTPException(status_code=500, detail="Erro ao aprovar notícias em lote.")


@router.get("/me")
def listar_noticias_por_current_user(
        page: int = Query(1, alias="page", ge=1),
        limit: int = Query(10, alias="limit", ge=1),
        status: str = Query(None),
        noticia_service: NoticiaService = Depends(get_noticia_service),
        current_user: UsuarioModel = Depends(get_current_user)
):
    offset = (page - 1) * limit
    filters: Dict[str, Any] = {}

    filters['USUARIO_ID'] = current_user.ID

    if status:
        filters['STATUS'] = status

    noticias, total_count = noticia_service.list(offset=None, limit=None, filters=filters)

    print('noticias', noticias)

    grouped: Dict[str, List[NoticiaRaspadaModel]] = defaultdict(list)
    for noticia in noticias:
        grouped[noticia.STATUS].append(noticia)

    total_pages = (total_count + limit - 1) // limit
    next_page = f"/noticias/me?page={page + 1}&limit={limit}&status={status}" if page < total_pages else None
    prev_page = f"/noticias/me?page={page - 1}&limit={limit}&status={status}" if page > 1 else None

    return {
        "total_count": total_count,
        "total_pages": total_pages,
        "page": page,
        "next": next_page,
        "previous": prev_page,
        "noticias": noticias,
        "data_agrupada_status": grouped
    }

@router.get("/categorias")
def listar_categorias(
    noticia_service: NoticiaService = Depends(get_noticia_service)
):
    return noticia_service.listar_categorias()

@router.post("/capturar-texto-noticia")
def capturar_texto_noticia(
    request: NoticiaRequest,
    noticia_service: NoticiaService = Depends(get_noticia_service)
):
    try:
        noticia = noticia_service.fetch_and_extract_text(request.url)

        updated_noticia = noticia_service.update_noticia_text(request.url, noticia)

        return updated_noticia  # Retorna a notícia atualizada
    except Exception as e:
        # Caso ocorra algum erro, lança uma exceção HTTP
        raise HTTPException(status_code=500, detail=f"Erro ao capturar texto da notícia: {e}")

@router.put("/{id}", response_model=dict)
def update_noticia(
    id: int,
    payload: NoticiaUpdateSchema,
    noticia_service: NoticiaService = Depends(get_noticia_service)
):
    try:
        updated = noticia_service.update(id, payload.dict())
    except Exception as e:
        raise HTTPException(status_code=404, detail=str(e))

    print('updated::', updated)

    return {
        "url": updated.URL,
        "fonte": updated.FONTE,
        "titulo": updated.TITULO,
        "categoria": updated.CATEGORIA,
        "regiao": updated.REGIAO,
        "uf": updated.UF,
        "reg_noticia": updated.REG_NOTICIA,
        "texto_noticia": updated.TEXTO_NOTICIA,
    }

@router.put("/set-current-user/{id}", response_model=dict)
def set_user_id(
    id: int,
    noticia_service: NoticiaService = Depends(get_noticia_service),
    current_user: UsuarioModel = Depends(get_current_user)
):
    try:
        payload = {
            "id_usuario": current_user.ID,
            "status": '07-EDIT-MODE'
        }
        updated = noticia_service.update(id, payload)
    except Exception as e:
        raise HTTPException(status_code=404, detail=str(e))

    print('updated::', updated)

    return {
        "url": updated.URL,
        "fonte": updated.FONTE,
        "titulo": updated.TITULO,
        "categoria": updated.CATEGORIA,
        "regiao": updated.REGIAO,
        "uf": updated.UF,
        "reg_noticia": updated.REG_NOTICIA,
        "texto_noticia": updated.TEXTO_NOTICIA,
    }

@router.get("/extrair-nomes/{id}")
def extrair_nomes(
        id: int,
        noticia_service: NoticiaService = Depends(get_noticia_service)
):
    return noticia_service.extrair_nomes(id)

@router.put("/{id}/nomes/batch")
def update_nomes_batch(
    id: int,
    payload: NoticiaNomesBatchUpdateIn,
    noticia_service: NoticiaService = Depends(get_noticia_service)
):
    try:
        itens = [i.dict(exclude_unset=True) for i in payload.nomes]
        result = noticia_service.update_nomes_many(id, itens)
        return result
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        print('aaaaaaaaaaaaa')
        print(e)
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/get-by-reg/{reg}")
def get_por_reg_noticia(
        reg: str,
        noticia_service: NoticiaService = Depends(get_noticia_service)
):
    return noticia_service.get_por_reg_noticia(reg)

@router.delete("/excluir-noticia/{id}")
def excluir_noticia(
        id: int,
        noticia_service: NoticiaService = Depends(get_noticia_service)
):
    return noticia_service.delete_by_id(id)


@router.get("/ping-task")
def trigger_ping():
    """
    Dispara uma tarefa simples e retorna o ID pra acompanhar depois.
    """
    task = ping.delay()
    return {"task_id": task.id, "message": "tarefa 'ping' enviada"}

@router.post("/sum")
def trigger_sum(a: int = Query(...), b: int = Query(...)):
    """
    Dispara a soma a+b como tarefa Celery.
    """
    task = add.delay(a, b)
    return {"task_id": task.id, "message": f"tarefa 'add' ({a}+{b}) enviada"}

@router.get("/tasks/{task_id}")
def get_task_status(task_id: str):
    """
    Consulta status e (se disponível) resultado da tarefa.
    """
    result = AsyncResult(task_id, app=celery_app)
    payload = {
        "task_id": task_id,
        "status": result.status,  # PENDING / STARTED / SUCCESS / FAILURE / RETRY
    }
    if result.ready():
        # result.result pode ser o valor retornado ou uma exceção, se falhou
        payload["result"] = result.result
    return payload

@router.get("/")
def root():
    return {
        "endpoints": {
            "GET /ping-task": "Dispara a tarefa 'ping'",
            "POST /sum?a=1&b=2": "Dispara a tarefa 'add'",
            "GET /tasks/{task_id}": "Consulta status/resultado",
        }
    }

@router.get("/debug/celery")
def debug():
    conf = celery_app.conf
    return {
        "broker_url": conf.broker_url,
        "result_backend": conf.result_backend,
        "queues": [q.name for q in conf.task_queues or []],
    }


@router.post("/transfer")
def trigger_transfer(
    date: str | None = Query(default=None, pattern=r"^\d{8}$"),
    category: str | None = Query(
        default=None,
        description="Nome ou abreviação: Crime|CR, Lavagem de Dinheiro|LD, Fraude|FF, Empresarial|SE, Ambiental|SA",
    ),
):
    job = transfer_task.delay(date_directory=date, category=category)
    return {"task_id": job.id, "message": "transfer agendada", "date": date, "category": category}