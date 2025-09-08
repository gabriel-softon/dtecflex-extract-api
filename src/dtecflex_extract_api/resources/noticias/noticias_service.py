import json
import os
import uuid
from collections import defaultdict
from dtecflex_extract_api.resources.noticias.schemas.noticia_create import NoticiaCreate
from dtecflex_extract_api.resources.noticias.schemas.noticia_nome_update import NoticiaNomePartialUpdate
import trafilatura
import requests
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright
from typing import Any, Dict, List, Optional, Tuple, Union
from datetime import datetime
from sqlalchemy import and_, case, text, func
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, joinedload
from src.dtecflex_extract_api.resources.noticias.entities.noticia_raspada import NoticiaRaspadaModel, \
    NoticiaRaspadaNomeModel
from openai import OpenAI
import re
import hashlib
from xml.sax.saxutils import escape
import xml.etree.ElementTree as ET

class NoticiaService:
    prompt_not_ambiental = """
        Você atuará como um interpretador avançado de textos jornalísticos e checador de fatos, com foco em identificar nomes de pessoas ou entidades envolvidas em crimes ou outros atos ilicitos.
        Seu objetivo é  localizar e extrair os nomes e as informações solicitadas, apresentando somente o resultado em formato de array JSON, onde cada nome será um elemento.
        O texto a ser analisado será fornecido entre as tags artigo.
        Para cada NOME, ENTIDADE ou EMPRESA encontrada no texto, resuma seu envolvimento em possíveis crimes.
        Em seguida, classifique cada um conforme o contexto de envolvimento no texto, utilizando um dos seguintes termos: acusado, suspeito, investigado, denunciado, condenado, preso ou réu.
        Inclua *APENAS* nomes próprios de pessoas, empresas ou entidades, evitando generalizações como função, profissão, etc.
        Não incluir nos resultados pessoas que não estejam diretamente envolvidas ou suspeitas de crime.
        Não inclua nomes de vítimas ou pessoas mencionadas como vítimas.
        A resposta não deve conter nenhum outro texto ou formatação além de um array de objetos JSON.

        Cada elemento do array deve conter as seguintes propriedades, mesmo que o valor seja null:

            NOME (nome da pessoa ou entidade encontrada na notícia)
            CPF (CPF para pessoa física ou CNPJ para pessoa jurídica encontrada na notícia)
            APELIDO
            NOME_CPF (fixo null)
            SEXO (usar 'M' para homens, 'F' para mulheres)
            PESSOA ('F' para pessoa física 'J' para pessoa jurídica ou entidades)
            IDADE (idade da pessoa, se encontrada no texto)
            ANIVERSARIO (data de nascimento da pessoa, se encontrada no texto )
            ATIVIDADE (ocupação, cargo ou atividade principal da pessoa)
            ENVOLVIMENTO (termo de classificação: acusado, suspeito, investigado, denunciado, condenado, preso, réu)
            OPERACAO (nome da operação policial ou judicial)
            FLG_PESSOA_PUBLICA (fixo false)
            INDICADOR_PPE (fixo false)
            ENVOLVIMENTO_GOV (retornar true se houver envolvimento com governo, ou false)

        Importante: Se não houver nenhum nome, retorne um array JSON vazio ( Exemplo: [] )
    """

    prompt_is_ambiental = """
        Você atuará como um interpretador avançado de textos jornalísticos e checador de fatos, com foco em identificar nomes de pessoas ou entidades envolvidas em atividades ambientais, incluindo crimes, infrações ou outros tipos de envolvimento com questões ambientais.
        Seu objetivo é localizar e extrair os nomes e as informações solicitadas, apresentando somente o resultado em formato de array JSON, onde cada nome será um elemento.
        O texto a ser analisado será fornecido entre as tags artigo.
        Para cada NOME, ENTIDADE ou EMPRESA encontrada no texto, resuma seu envolvimento em possíveis crimes, infrações ambientais, projetos, denúncias ou outras questões relacionadas a atividades ambientais.
        Em seguida, classifique cada um conforme o contexto de envolvimento no texto, utilizando um dos seguintes termos: acusado, suspeito, investigado, denunciado, condenado, preso, réu, envolvido, colaborador, responsável, líder, organizador, participante ou outros termos relacionados à categoria ambiental.
        Inclua *APENAS* nomes próprios de pessoas, empresas ou entidades, evitando generalizações como função, profissão, etc.
        Não incluir nos resultados pessoas que não estejam diretamente envolvidas ou suspeitas de crimes ou infrações ambientais.
        Não inclua nomes de vítimas ou pessoas mencionadas como vítimas.
        A resposta não deve conter nenhum outro texto ou formatação além de um array de objetos JSON.

        Cada elemento do array deve conter as seguintes propriedades, mesmo que o valor seja null:

            NOME (nome da pessoa ou entidade encontrada na notícia)
            CPF (CPF para pessoa física ou CNPJ para pessoa jurídica encontrada na notícia)
            APELIDO
            NOME_CPF (fixo null)
            SEXO (usar 'M' para homens, 'F' para mulheres)
            PESSOA ('F' para pessoa física, 'J' para pessoa jurídica ou entidades)
            IDADE (idade da pessoa, se encontrada no texto)
            ANIVERSARIO (data de nascimento da pessoa, se encontrada no texto)
            ATIVIDADE (ocupação, cargo ou atividade principal da pessoa)
            ENVOLVIMENTO (termo de classificação: acusado, suspeito, investigado, denunciado, condenado, preso, réu, envolvido, colaborador, responsável, líder, organizador, participante, etc.)
            OPERACAO (nome da operação policial ou judicial, se houver)
            FLG_PESSOA_PUBLICA (fixo false)
            INDICADOR_PPE (fixo false)
            ENVOLVIMENTO_GOV (retornar true se houver envolvimento com o governo, ou false)

        Importante: Se não houver nenhum nome, retorne um array JSON vazio (Exemplo: []).
    """

    def __init__(self, session: Session, user_agent=None, timeout=30, model: str = 'gpt-4o', notice_categoria: str = 'normal'):
        self.session = session
        self.user_agent = user_agent or (
            'Mozilla/5.0 (Linux; Android 13; SM-S901B) '
            'AppleWebKit/537.36 (KHTML, like Gecko) '
            'Chrome/112.0.0.0 Mobile Safari/537.36'
        )
        self.timeout = timeout  # em segundos
        self.client = OpenAI()
        self.notice_categoria = notice_categoria
        self.model = model

        if self.notice_categoria == 'Ambiental' or self.notice_categoria == 'Empresarial':
            self.prompt = self.prompt_is_ambiental
        else:
            self.prompt = self.prompt_not_ambiental

    def get_by_id(self, id: int):
        noticia = (
            self.session
                .query(NoticiaRaspadaModel)
                .filter(NoticiaRaspadaModel.ID == id)
                .first()
        )
        if not noticia:
            raise Exception("Noticia não encontrada")
        return noticia

    def create_nome(self, schema) -> NoticiaRaspadaNomeModel:
        obj = NoticiaRaspadaNomeModel(
            NOTICIA_ID         = schema.noticia_id,
            CPF                = schema.cpf,
            APELIDO            = schema.apelido,
            NOME_CPF           = schema.nome_cpf,
            NOME               = schema.nome,
            OPERACAO           = schema.operacao,
            SEXO               = schema.sexo,
            PESSOA             = schema.pessoa,
            IDADE              = schema.idade,
            ATIVIDADE          = schema.atividade,
            ENVOLVIMENTO       = schema.envolvimento,
            TIPO_SUSPEITA      = schema.tipo_suspeita,
            FLG_PESSOA_PUBLICA = schema.flg_pessoa_publica,
            ANIVERSARIO        = schema.aniversario,
            INDICADOR_PPE      = schema.indicador_ppe,
            # ENVOLVIMENTO_GOV   = schema.envolvimento_gov
        )
        self.session.add(obj)
        self.session.commit()
        self.session.refresh(obj)
        return obj

    def delete_nome(self, nome_id: int) -> None:
        obj = self.session.query(NoticiaRaspadaNomeModel).filter_by(ID=nome_id).first()
        if not obj:
            raise ValueError(f"Nome com ID {nome_id} não encontrado")
        self.session.delete(obj)
        self.session.commit()

    def listar_categorias(self) -> List[str]:
        query = self.session.query(NoticiaRaspadaModel.CATEGORIA).distinct()
        fontes = [row.CATEGORIA for row in query.all()]
        return fontes

    def create(self, payload: 'NoticiaCreate') -> NoticiaRaspadaModel:
        hash = hashlib.sha256(uuid.uuid4().bytes).hexdigest()[:16]
        entity = NoticiaRaspadaModel(
            LINK_ID = hash,
            URL = payload.url,
            FONTE = payload.fonte,
            CATEGORIA = payload.categoria,
            DATA_PUBLICACAO = payload.data_publicacao,
            REGIAO = payload.regiao,
            UF = payload.uf,
            REG_NOTICIA = payload.reg_noticia,
            QUERY = "",
            TITULO = payload.titulo,
            STATUS = payload.status,
            TEXTO_NOTICIA = payload.texto_noticia,
            DT_RASPAGEM = datetime.now(),
            ID_ORIGINAL = hash,
            LINK_ORIGINAL = payload.link_original,
        )
        try:
            self.session.add(entity)
            self.session.commit()
            self.session.refresh(entity)
            return entity
        except IntegrityError as e:
            self.session.rollback()
            raise ValueError("LINK_ID já existe (URL duplicada).") from e

    def list(
        self,
        offset: int = 0,
        limit: int = 10,
        filters: Optional[Dict[str, Any]] = None,
        incluir_aux: bool = False,
    ) -> Tuple[List[NoticiaRaspadaModel], int]:
        query = (
            self.session.query(NoticiaRaspadaModel)
            .options(joinedload(NoticiaRaspadaModel.nomes_raspados))
        )

        if filters:
            filter_conditions = []

            if 'STATUS' in filters and filters['STATUS']:
                filter_conditions.append(NoticiaRaspadaModel.STATUS.in_(filters['STATUS']))

            if 'DT_APROVACAO' in filters:
                data_inicio, data_fim = filters['DT_APROVACAO']
                if data_inicio and data_fim:
                    filter_conditions.append(NoticiaRaspadaModel.DT_APROVACAO.between(data_inicio, data_fim))
                elif data_inicio:
                    filter_conditions.append(NoticiaRaspadaModel.DT_APROVACAO >= data_inicio)
                elif data_fim:
                    filter_conditions.append(NoticiaRaspadaModel.DT_APROVACAO <= data_fim)

            if 'DATA_PUBLICACAO' in filters:
                data_inicio, data_fim = filters['DATA_PUBLICACAO']
                if data_inicio and data_fim:
                    filter_conditions.append(NoticiaRaspadaModel.DATA_PUBLICACAO.between(data_inicio, data_fim))
                elif data_inicio:
                    filter_conditions.append(NoticiaRaspadaModel.DATA_PUBLICACAO >= data_inicio)
                elif data_fim:
                    filter_conditions.append(NoticiaRaspadaModel.DATA_PUBLICACAO <= data_fim)

            if 'FONTE' in filters and filters['FONTE']:
                filter_conditions.append(NoticiaRaspadaModel.FONTE.ilike(f"%{filters['FONTE']}%"))

            if 'CATEGORIA' in filters and filters['CATEGORIA']:
                filter_conditions.append(NoticiaRaspadaModel.CATEGORIA == filters['CATEGORIA'])

            if 'SUBCATEGORIA' in filters and filters['SUBCATEGORIA']:
                subcategorias = " ".join(filters['SUBCATEGORIA'])
                query = query.filter(
                    text("MATCH(QUERY) AGAINST (:subcategorias IN BOOLEAN MODE)")
                ).params(subcategorias=subcategorias)

            if 'REG_NOTICIA_RANGE' in filters and filters['REG_NOTICIA_RANGE']:
                lo, hi = filters['REG_NOTICIA_RANGE']
                query = query.filter(
                    NoticiaRaspadaModel.REG_NOTICIA >= lo,
                    NoticiaRaspadaModel.REG_NOTICIA < hi
                )

            if 'USUARIO_ID' in filters and filters['USUARIO_ID']:
                filter_conditions.append(NoticiaRaspadaModel.ID_USUARIO == filters['USUARIO_ID'])

            if filter_conditions:
                query = query.filter(and_(*filter_conditions))

        total_count = query.count()
        noticias = (
            query.order_by(NoticiaRaspadaModel.ID.desc())
                 .offset(offset)
                 .limit(limit)
                 .all()
        )

        if incluir_aux and noticias:
            registros = [n.REG_NOTICIA for n in noticias if getattr(n, "REG_NOTICIA", None)]
            aux_por_reg = self._fetch_aux_by_registros(registros)

            for n in noticias:
                setattr(n, "aux_registros", aux_por_reg.get(n.REG_NOTICIA, []))

        return noticias, total_count
    
    def update(self, id: int, data: Dict[str, Any]) -> NoticiaRaspadaModel:
        noticia = (
            self.session
                .query(NoticiaRaspadaModel)
                .filter(NoticiaRaspadaModel.ID == id)
                .first()
        )
        if not noticia:
            raise Exception(f"Notícia com URL {id} não encontrada")

        mapping = {
            "fonte": "FONTE",
            "titulo": "TITULO",
            "categoria": "CATEGORIA",
            "regiao": "REGIAO",
            "id_usuario": "ID_USUARIO",
            "uf": "UF",
            "reg_noticia": "REG_NOTICIA",
            "texto_noticia": "TEXTO_NOTICIA",
            "status": "STATUS",
        }

        for campo_payload, valor in data.items():
            if valor is None:
                continue
            attr = mapping.get(campo_payload)
            if not attr:
                continue
            setattr(noticia, attr, valor)

        self.session.commit()
        self.session.refresh(noticia)
        return noticia

    def update_nomes_many(self, noticia_id: int, items: List[Dict[str, Any]]) -> Dict[str, Any]:
        s = self.session

        # garante que a notícia existe
        noticia = s.get(NoticiaRaspadaModel, noticia_id)
        if not noticia:
            raise ValueError("Notícia não encontrada.")

        ids = [i["id"] for i in items if "id" in i and i["id"] is not None]
        if not ids:
            return {"updated": 0, "updated_ids": [], "not_found": [], "wrong_noticia": [], "skipped": []}

        rows = (
            s.query(NoticiaRaspadaNomeModel)
             .filter(NoticiaRaspadaNomeModel.ID.in_(ids))
             .all()
        )
        by_id = {r.ID: r for r in rows}

        updated_ids, not_found, wrong_noticia, skipped = [], [], [], []

        for dto in items:
            nid = dto.get("id")
            if not nid:
                skipped.append(nid)
                continue

            model = by_id.get(nid)
            if not model:
                not_found.append(nid)
                continue

            if model.NOTICIA_ID != noticia_id:
                wrong_noticia.append(nid)
                continue

            # aplica somente campos presentes
            if "nome" in dto: model.NOME = dto["nome"]
            if "cpf" in dto: model.CPF = dto["cpf"] or None
            if "apelido" in dto: model.APELIDO = dto["apelido"] or None
            if "nome_cpf" in dto: model.NOME_CPF = dto["nome_cpf"] or None
            if "operacao" in dto: model.OPERACAO = dto["operacao"] or None
            if "sexo" in dto: model.SEXO = dto["sexo"] or None
            if "pessoa" in dto: model.PESSOA = dto["pessoa"] or None
            if "idade" in dto: model.IDADE = dto["idade"]
            if "atividade" in dto: model.ATIVIDADE = dto["atividade"] or None
            if "envolvimento" in dto: model.ENVOLVIMENTO = dto["envolvimento"] or None
            if "tipo_suspeita" in dto: model.TIPO_SUSPEITA = dto["tipo_suspeita"] or None
            if "flg_pessoa_publica" in dto: model.FLG_PESSOA_PUBLICA = self._bool_to_flag(dto["flg_pessoa_publica"])
            if "indicador_ppe" in dto: model.INDICADOR_PPE = self._bool_to_flag(dto["indicador_ppe"])
            if "aniversario" in dto: model.ANIVERSARIO = dto["aniversario"]

            updated_ids.append(nid)

        s.commit()

        return {
            "updated": len(updated_ids),
            "updated_ids": updated_ids,
            "not_found": not_found,
            "wrong_noticia": wrong_noticia,
            "skipped": skipped
        }

    def update_nome(self, nome_id: int, dto: "NoticiaNomePartialUpdate") -> NoticiaRaspadaNomeModel:
        # garante que o ID do body confere com a rota (já que seu schema exige id)
        if dto.id != nome_id:
            raise ValueError("ID do payload não confere com o ID da rota.")

        obj = (
            self.session
            .query(NoticiaRaspadaNomeModel)
            .filter(NoticiaRaspadaNomeModel.ID == nome_id)
            .first()
        )
        if not obj:
            raise ValueError(f"Nome com ID {nome_id} não encontrado")

        # Compatível com Pydantic v1 e v2
        fields_set = getattr(dto, 'model_fields_set', getattr(dto, '__fields_set__', set()))

        # atualiza apenas os campos presentes no payload
        if 'nome' in fields_set:                 obj.NOME  = dto.nome
        if 'cpf' in fields_set:                  obj.CPF   = dto.cpf
        if 'apelido' in fields_set:              obj.APELIDO = dto.apelido
        if 'nome_cpf' in fields_set:             obj.NOME_CPF = dto.nome_cpf
        if 'operacao' in fields_set:             obj.OPERACAO = dto.operacao
        if 'sexo' in fields_set:                 obj.SEXO  = dto.sexo   # 'F'|'M'|None
        if 'pessoa' in fields_set:               obj.PESSOA = dto.pessoa # 'PF'|'PJ'|None
        if 'idade' in fields_set:                obj.IDADE = dto.idade
        if 'atividade' in fields_set:            obj.ATIVIDADE = dto.atividade
        if 'envolvimento' in fields_set:         obj.ENVOLVIMENTO = dto.envolvimento
        if 'tipo_suspeita' in fields_set:        obj.TIPO_SUSPEITA = dto.tipo_suspeita
        if 'flg_pessoa_publica' in fields_set:   obj.FLG_PESSOA_PUBLICA = self._bool_to_flag(dto.flg_pessoa_publica)
        if 'indicador_ppe' in fields_set:        obj.INDICADOR_PPE      = self._bool_to_flag(dto.indicador_ppe)
        if 'aniversario' in fields_set:          obj.ANIVERSARIO        = dto.aniversario  # date|None

        self.session.commit()
        self.session.refresh(obj)
        return obj
    def aprovar_em_lote(self, ids: List[int]) -> Dict[str, Any]:
        ids = list({int(i) for i in ids if i is not None})

        if not ids:
            return {
                "status_set": "201-APPROVED",
                "updated": 0,
                "updated_ids": [],
                "not_found": []
            }

        existentes = [
            row[0]
            for row in self.session.query(NoticiaRaspadaModel.ID)
                .filter(NoticiaRaspadaModel.ID.in_(ids))
                .all()
        ]
        nao_encontrados = [i for i in ids if i not in existentes]

        atualizados = 0
        if existentes:
            try:
                atualizados = (
                    self.session.query(NoticiaRaspadaModel)
                    .filter(NoticiaRaspadaModel.ID.in_(existentes))
                    .update(
                        {
                            NoticiaRaspadaModel.STATUS: "201-APPROVED",
                            NoticiaRaspadaModel.DT_APROVACAO: func.now(),
                        },
                        synchronize_session=False
                    )
                )
                self.session.commit()
            except Exception:
                self.session.rollback()
                raise

        return {
            "status_set": "201-APPROVED",
            "updated": atualizados,
            "updated_ids": existentes,
            "not_found": nao_encontrados
        }

    def update_noticia_text(self, url: str, text: str):
        try:
            noticia = (
                self.session
                .query(NoticiaRaspadaModel)
                .filter(NoticiaRaspadaModel.URL == url)
                .first()
            )
            if not noticia:
                raise Exception(f"Notícia com URL {url} não encontrada")

            noticia.TEXTO_NOTICIA = text

            self.session.commit()

            self.session.refresh(noticia)

            return noticia

        except Exception as e:
            self.session.rollback()
            print(f"Erro ao atualizar texto da notícia: {e}")
            raise

    def delete_by_id(self, id: str) -> None:
        noticia = (
            self.session
                .query(NoticiaRaspadaModel)
                .filter(NoticiaRaspadaModel.ID == id)
                .first()
        )
        if not noticia:
            raise Exception(f"Notícia com URL {id} não encontrada")
        self.session.delete(noticia)
        self.session.commit()

    def fetch_and_extract_text(self, url: str) -> str:
        html = self._fetch(url)
        if not html:
            return ""
        text = trafilatura.extract(html, include_comments=False)
        return text or ""

    def extrair_nomes(self, id) -> list:
        noticia = (
            self.session
                .query(NoticiaRaspadaModel)
                .filter(NoticiaRaspadaModel.ID == id)
                .first()
        )

        text = noticia.TEXTO_NOTICIA
        if not noticia:
            raise Exception(f"Notícia com URL {id} não encontrada")

        try:
            artigo = f"<artigo>\n{text}\n</artigo>"
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": self.prompt},
                    {"role": "user", "content": artigo}
                ]
            )
            no_none_name = []

            resposta = response.choices[0].message.content.strip()

            print(f"Resposta recebida: {resposta}")

            match = re.search(r'```json\s*(\[\s*{.*}\s*\])\s*```', resposta, re.DOTALL)
            if match:
                json_str = match.group(1)
            else:
                json_str = resposta

            if json_str:
                try:
                    resposta_dict = json.loads(json_str)
                except json.JSONDecodeError as json_err:
                    print(f"Erro ao decodificar JSON: {json_err}")
                    return []
            else:
                print("Resposta vazia ou formato inesperado.")
                return []

            if isinstance(resposta_dict, list):
                for rd in resposta_dict:
                    if rd.get('NOME'):
                        no_none_name.append(rd)
            else:
                print("A resposta JSON não é uma lista conforme esperado.")
                return []

            return no_none_name

        except Exception as e:
            print(f"Exceção geral: {e}")
            return []

    def get_por_reg_noticia(self, reg):
        noticia = (
            self.session
                .query(NoticiaRaspadaModel)
                .filter(NoticiaRaspadaModel.REG_NOTICIA == reg)
                .first()
        )

        return noticia

    def buscar_no_dtec(self, nome: str, rows: int = 20) -> List[Dict[str, str]]:
            cliente = os.getenv("DTEC_CLIENTE")
            usuario = os.getenv("DTEC_USUARIO")
            senha = os.getenv("DTEC_SENHA")
            url = os.getenv("DTEC_URL", "https://dtec-flex.com.br/dtecflexWS/rest/x/search")

            if not all([cliente, usuario, senha]):
                raise ValueError("Credenciais DTEC não configuradas (DTEC_CLIENTE/USUARIO/SENHA).")

            # Escapa caracteres especiais para uso em XML
            nome_q = escape((nome or "").strip(), {"'": "&apos;", '"': "&quot;"})

            body = f"""<?xml version="1.0" encoding="UTF-8"?>
    <consulta>
      <cliente>{cliente}</cliente>
      <usuario>{usuario}</usuario>
      <senha>{senha}</senha>
      <qry>nome:{nome_q}</qry>
      <options>rows:{rows}</options>
    </consulta>"""

            try:
                resp = requests.post(
                    url,
                    data=body,
                    headers={"Content-Type": "application/xml"},
                    timeout=10,
                )
                resp.raise_for_status()
            except requests.Timeout as e:
                raise
            except requests.RequestException as e:
                raise

            try:
                root = ET.fromstring(resp.content)
            except ET.ParseError as e:
                raise

            results: List[Dict[str, str]] = []
            for doc in root.findall(".//resultList/doc"):
                item = {
                    (attr.findtext("name") or ""): (attr.findtext("value") or "")
                    for attr in doc.findall("attribute")
                }
                item = {k: v for k, v in item.items() if k}
                results.append(item)

            return results

    def _fetch(self, url: str) -> str:
        html = self._fetch_with_requests(url)
        if html is None:
            html = self._fetch_with_playwright(url)
        return html or ""

    def _fetch_with_requests(self, url: str) -> str | None:
        headers = {'User-Agent': self.user_agent}
        try:
            r = requests.get(url, headers=headers, timeout=self.timeout)
            if r.status_code == 200:
                return r.text
        except requests.exceptions.Timeout:
            print(f"Timeout requests: {url}")
        except requests.RequestException as e:
            print(f"Erro requests: {e}")
        return None

    def _fetch_with_playwright(self, url: str) -> str | None:
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                page = browser.new_page(user_agent=self.user_agent)
                page.goto(url, timeout=self.timeout * 1000)
                page.wait_for_load_state('networkidle', timeout=self.timeout * 1000)
                content = page.content()
                browser.close()
                return content
        # except PlaywrightTimeoutError:
        #     print(f"Timeout Playwright: {url}")
        except Exception as e:
            print(f"Erro Playwright: {e}")
        return None

    def _make_link_id(self, url: str) -> str:
        if not url:
            raise ValueError("URL obrigatória para gerar LINK_ID")
        return hashlib.sha256(url.encode("utf-8")).hexdigest()

    def _bool_to_flag(self, v: Optional[Union[bool, str, int]]) -> Optional[str]:
        if v is None:
            return None
        if isinstance(v, bool):
            return '1' if v else '0'
        if isinstance(v, int):
            return '1' if v != 0 else '0'
        if isinstance(v, str):
            s = v.strip().lower()
            if s in ('1', 'true', 't', 'yes', 'y', 'sim'):
                return '1'
            if s in ('0', 'false', 'f', 'no', 'n', 'nao', 'não'):
                return '0'
            # fallback: qualquer string não vazia vira '1'
            return '1' if s else '0'
        return '1' if v else '0'

    def _flag_to_bool(self, v: Optional[Union[str, int, bool]]) -> Optional[bool]:
        if v is None:
            return None
        if isinstance(v, bool):
            return v
        if isinstance(v, int):
            return bool(v)
        if isinstance(v, str):
            return v.strip() == '1'
        return None
    
    def _fetch_aux_by_registros(self, registros: List[str], batch_size: int = 1000) -> Dict[str, List[Dict[str, Any]]]:
        if not registros:
            return {}

        uniq = list(dict.fromkeys(registros))
        agrupado: Dict[str, List[Dict[str, Any]]] = defaultdict(list)

        for start in range(0, len(uniq), batch_size):
            batch = uniq[start:start + batch_size]

            placeholders = ", ".join(f":p{i}" for i in range(len(batch)))
            sql = text(f"SELECT * FROM Auxiliar WHERE REGISTRO_NOTICIA IN ({placeholders})")

            params = {f"p{i}": val for i, val in enumerate(batch)}
            result = self.session.execute(sql, params)

            for row in result.mappings():
                reg = row.get("REGISTRO_NOTICIA")
                agrupado[reg].append(dict(row))

        return agrupado