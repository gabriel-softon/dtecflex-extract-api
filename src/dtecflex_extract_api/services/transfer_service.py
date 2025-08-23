import os
import glob
import subprocess
from collections import defaultdict
from datetime import datetime
from typing import Any, Dict, List, Tuple
import mysql.connector

from src.dtecflex_extract_api.config.celery import settings
from src.dtecflex_extract_api.config.celery import celery_app  # <-- importa o app do Celery

# Mapeamentos
CAT_ABREV = {
    'Lavagem de Dinheiro': 'LD',
    'Crime':               'CR',
    'Fraude':              'FF',
    'Empresarial':         'SE',
    'Ambiental':           'SA'
}
CAT_PREFIX = { 'LD': 'N', 'CR': 'C', 'FF': 'N', 'SE': 'E', 'SA': 'A' }
CATEGORY_MAPPING = {
    'Lavagem de Dinheiro': ('Lavagem de Dinheiro', 'DTECFLEX'),
    'Crime':               ('Crimes',              'DTECCRIM'),
    'Fraude':              ('Fraude Financeira',   'DTECFLEX'),
    'Empresarial':         ('Saúde Empresarial',   'DTECEMP'),
    'Ambiental':           ('SocioAmbiental',      'DTECAMB'),
}

def _db_conn():
    return mysql.connector.connect(
        user=settings.DB_USER,
        password=settings.DB_PASS,
        host=settings.DB_HOST,
        port=settings.DB_PORT,
        database=settings.DB_NAME,
    )

INV_CAT_ABREV = {v: k for k, v in CAT_ABREV.items()}

def normalize_category(cat: str) -> tuple[str, str, str]:
    if not cat:
        raise ValueError("Categoria vazia.")
    cat = cat.strip()
    if cat in CAT_ABREV:  # nome por extenso
        abrev = CAT_ABREV[cat]
        return abrev, CAT_PREFIX.get(abrev), cat
    abrev = cat.upper()   # abreviação (CR, LD, FF, SE, SA)
    if abrev in INV_CAT_ABREV:
        return abrev, CAT_PREFIX.get(abrev), INV_CAT_ABREV[abrev]
    raise ValueError(f"Categoria inválida: {cat}")

def fetch_registros(logger, reg_like: str | None = None) -> List[Dict[str, Any]]:
    try:
        conn = _db_conn()
        cursor = conn.cursor(dictionary=True)
        sql = """
            SELECT *
            FROM TB_NOTICIA_RASPADA
            WHERE STATUS = '201-APPROVED'
        """
        params = []
        if reg_like:
            sql += " AND REG_NOTICIA LIKE %s"
            params.append(reg_like)
        cursor.execute(sql, params)
        registros = cursor.fetchall()
        for reg in registros:
            reg['CAT_ABREV']  = CAT_ABREV.get(reg['CATEGORIA'])
            reg['CAT_PREFIX'] = CAT_PREFIX.get(reg['CAT_ABREV'])
        return registros
    except mysql.connector.Error as err:
        logger.error(f"Erro no banco de dados: {err}")
        return []
    finally:
        try:
            cursor.close(); conn.close()
        except Exception:
            pass

def agrupar_registros(registros: List[Dict[str, Any]]):
    grupos = defaultdict(list)
    for reg in registros:
        grupos[reg['CATEGORIA']].append(reg)
    return [{"CATEGORIA": cat, "REGISTROS": regs} for cat, regs in grupos.items()]

def fetch_nomes_por_noticia(noticia_id: int, logger) -> List[Dict[str, Any]]:
    try:
        conn = _db_conn()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("""
            SELECT 
                ID AS name_id,
                NOME, CPF, NOME_CPF, APELIDO,
                SEXO, PESSOA, IDADE, ATIVIDADE,
                ENVOLVIMENTO, TIPO_SUSPEITA,
                FLG_PESSOA_PUBLICA, ANIVERSARIO,
                INDICADOR_PPE, OPERACAO
            FROM TB_NOTICIA_RASPADA_NOME
            WHERE NOTICIA_ID = %s
        """, (noticia_id,))
        return cursor.fetchall()
    except mysql.connector.Error as err:
        logger.error(f"Erro ao buscar nomes da notícia {noticia_id}: {err}")
        return []
    finally:
        try:
            cursor.close(); conn.close()
        except Exception:
            pass

def load_news(news_id: int) -> Dict[str, Any]:
    """Busca os campos mínimos da notícia para popular a Auxiliar."""
    conn = _db_conn()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("""
            SELECT ID, URL, FONTE, DATA_PUBLICACAO, CATEGORIA, REG_NOTICIA,
                   TEXTO_NOTICIA, UF, REGIAO, OPERACAO, TITULO
            FROM TB_NOTICIA_RASPADA
            WHERE ID = %s
        """, (news_id,))
        row = cur.fetchone()
        return row or {}
    finally:
        try:
            cur.close(); conn.close()
        except Exception:
            pass

def construir_caminhos(registro: Dict[str, Any], date_dir: str) -> Tuple[str, str]:
    local_pattern = f"{settings.MEDIA_BASE}/{registro['CAT_ABREV']}/{registro['CAT_PREFIX']}{date_dir}/{registro['REG_NOTICIA']}*"
    remote_dir    = f"{settings.REMOTE_BASE}/{registro['CAT_ABREV']}/{registro['CAT_PREFIX']}{date_dir}"
    return local_pattern, remote_dir

def _ssh_prefix() -> str:
    return f'ssh -i {settings.SSH_KEY_PATH} -p {settings.SSH_PORT}'

def transferir_arquivo(local_pattern: str, remote_dir: str, noticia_id: int, logger) -> bool:
    itens = glob.glob(local_pattern)
    if not itens:
        logger.warning(f"Nenhum item encontrado para o padrão: {local_pattern}")
        return False

    mkdir_cmd = f'{_ssh_prefix()} {settings.SSH_USER}@{settings.SSH_HOST} "mkdir -p {remote_dir}"'
    mkdir_res = subprocess.run(mkdir_cmd, shell=True, capture_output=True, text=True)
    if mkdir_res.returncode != 0:
        logger.error(f"Erro ao criar diretório remoto {remote_dir}: {mkdir_res.stderr}")
        return False

    itens_str = " ".join(f'"{item}"' for item in itens)
    rsync_cmd = (
        f'rsync -az --no-perms --no-owner --no-group --no-times --omit-dir-times --size-only '
        f'-e "{_ssh_prefix()}" {itens_str} {settings.SSH_USER}@{settings.SSH_HOST}:{remote_dir}'
    )
    logger.info(f"Executando rsync: {rsync_cmd}")
    result = subprocess.run(rsync_cmd, shell=True, capture_output=True, text=True)

    if result.returncode == 0:
        try:
            conn = _db_conn()
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE TB_NOTICIA_RASPADA SET STATUS=%s, DT_TRANSFERENCIA=NOW() WHERE ID=%s",
                ("205-TRANSFERED", noticia_id),
            )
            conn.commit()
            logger.info(f"Notícia {noticia_id} -> 205-TRANSFERED")
        except mysql.connector.Error as err:
            logger.error(f"Erro ao atualizar TB_NOTICIA_RASPADA: {err}")
        finally:
            try:
                cursor.close()
                conn.close()
            except Exception:
                pass
        return True

    logger.error(f"Erro na transferência para {local_pattern}: {result.stderr}")
    return False

def fetch_noticias_publicadas(logger):
    try:
        conn = _db_conn()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("""
            SELECT 
                r.ID AS news_id,
                r.LINK_ID, r.URL, r.FONTE, r.DATA_PUBLICACAO,
                r.CATEGORIA, r.REG_NOTICIA, r.QUERY,
                r.ID_ORIGINAL, r.LINK_ORIGINAL,
                r.DT_RASPAGEM, r.DT_DECODE, r.TITULO,
                r.ID_USUARIO, r.STATUS,
                r.TENTATIVA_EXTRAIR, r.TEXTO_NOTICIA,
                r.REGIAO, r.UF, r.DT_APROVACAO,
                n.ID AS name_id,
                n.NOME, n.CPF, n.NOME_CPF, n.APELIDO,
                n.SEXO, n.PESSOA, n.IDADE, n.ATIVIDADE,
                n.ENVOLVIMENTO, n.TIPO_SUSPEITA,
                n.FLG_PESSOA_PUBLICA, n.ANIVERSARIO,
                n.INDICADOR_PPE,
                n.OPERACAO AS OPERACAO
            FROM TB_NOTICIA_RASPADA r
            LEFT JOIN TB_NOTICIA_RASPADA_NOME n 
                ON r.ID = n.NOTICIA_ID
            WHERE r.STATUS = '205-TRANSFERED'
              AND r.DT_TRANSFERENCIA >= CURRENT_DATE
        """)
        rows = cursor.fetchall()
        noticias = {}
        for row in rows:
            nid = row['news_id']
            if nid not in noticias:
                noticias[nid] = {
                    'ID': nid,
                    'LINK_ID': row.get('LINK_ID'),
                    'URL': row.get('URL'),
                    'FONTE': row.get('FONTE'),
                    'DATA_PUBLICACAO': row.get('DATA_PUBLICACAO'),
                    'CATEGORIA': row.get('CATEGORIA'),
                    'REG_NOTICIA': row.get('REG_NOTICIA'),
                    'TEXTO_NOTICIA': row.get('TEXTO_NOTICIA'),
                    'UF': row.get('UF'),
                    'REGIAO': row.get('REGIAO'),
                    'OPERACAO': row.get('OPERACAO'),
                    'TITULO': row.get('TITULO'),
                    'NAMES': []
                }
            if row.get('name_id') is not None:
                noticias[nid]['NAMES'].append({
                    'NOME': row.get('NOME'),
                    'CPF': row.get('CPF'),
                    'NOME_CPF': row.get('NOME_CPF'),
                    'APELIDO': row.get('APELIDO'),
                    'SEXO': row.get('SEXO'),
                    'PESSOA': row.get('PESSOA'),
                    'OPERACAO': row.get('OPERACAO'),
                    'IDADE': row.get('IDADE'),
                    'ATIVIDADE': row.get('ATIVIDADE'),
                    'ENVOLVIMENTO': row.get('ENVOLVIMENTO'),
                    'FLG_PESSOA_PUBLICA': row.get('FLG_PESSOA_PUBLICA'),
                    'ANIVERSARIO': row.get('ANIVERSARIO'),
                    'INDICADOR_PPE': row.get('INDICADOR_PPE'),
                })
        return list(noticias.values())
    except mysql.connector.Error as err:
        logger.error(f"Erro no banco de dados: {err}")
        return []
    finally:
        try:
            cursor.close()
            conn.close()
        except Exception:
            pass

def insert_names_to_aux_for_news(news: Dict[str, Any], names: List[Dict[str, Any]], logger, *, chunk_size: int = 50) -> Tuple[int, bool]:
    """
    Insere nomes em lotes pequenos (executemany) com commit curto e fallback por linha.
    Retorna (qtde_inserida, publicou_bool).
    """
    if not names:
        return 0, False

    conn = _db_conn()
    cursor = conn.cursor()
    try:
        conn.autocommit = False
        # timeouts curtos só nesta sessão (evita "colar" indefinidamente)
        try:
            cursor.execute("SET SESSION innodb_lock_wait_timeout = 15")
            cursor.execute("SET SESSION lock_wait_timeout = 15")
            cursor.execute("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
        except Exception as e:
            logger.warning(f"[aux] não consegui ajustar timeouts de sessão: {e}")

        insert_sql = """
            INSERT INTO Auxiliar (
                NOME, CPF, NOME_CPF, APELIDO, DTEC,
                SEXO, PESSOA, IDADE, ATIVIDADE, ENVOLVIMENTO,
                TIPO_SUSPEITA, OPERACAO, TITULO, DATA_NOTICIA, FONTE_NOTICIA,
                REGIAO, ESTADO, REGISTRO_NOTICIA, FLG_PESSOA_PUBLICA, DATA_GRAVACAO,
                EXISTEM_PROCESSOS, ORIGEM_UF, TRIBUNAIS, LINKS_TRIBUNAIS, DATA_PESQUISA,
                TIPO_INFORMACAO, ANIVERSARIO, CITACOES_NA_MIDIA, INDICADOR_PPE, PEP_RELACIONADO,
                LINK_NOTICIA, DATA_ATUALIZACAO, ORGAO, EMPRESA_RELACIONADA, CNPJ_EMPRESA_RELACIONADA,
                RELACIONAMENTO, DATA_INICIO_MANDATO, DATA_FIM_MANDATO, DATA_CARENCIA
            ) VALUES (
                %s,%s,%s,%s,%s,
                %s,%s,%s,%s,%s,
                %s,%s,%s,%s,%s,
                %s,%s,%s,%s,NOW(),
                %s,%s,%s,%s,NOW(),
                %s,%s,%s,%s,%s,
                %s,NOW(),%s,%s,%s,
                %s,%s,%s,%s
            )
        """

        categoria = news.get("CATEGORIA")
        tipo_suspeita, tipo_informacao = CATEGORY_MAPPING.get(categoria, (None, None))

        base = dict(
            titulo=news.get("TITULO"),
            data_noticia=news.get("DATA_PUBLICACAO"),
            fonte=news.get("FONTE"),
            regiao=news.get("REGIAO"),
            estado=news.get("UF"),
            reg=news.get("REG_NOTICIA"),
            texto=news.get("TEXTO_NOTICIA"),
            url=news.get("URL"),
        )

        # campos fixos None
        dtec = existem_processos = origem_uf = tribunais = links_tribunais = None
        pep_rel = orgao = emp_rel = cnpj_emp_rel = None
        relacionamento = dt_ini = dt_fim = dt_car = None

        batch = []
        for nm in names:
            batch.append((
                nm['NOME'], nm['CPF'], nm['NOME_CPF'], nm['APELIDO'], dtec,
                nm['SEXO'], nm['PESSOA'], nm['IDADE'], nm['ATIVIDADE'], nm['ENVOLVIMENTO'],
                tipo_suspeita, nm['OPERACAO'], base["titulo"], base["data_noticia"], base["fonte"],
                base["regiao"], base["estado"], base["reg"], nm['FLG_PESSOA_PUBLICA'],
                existem_processos, origem_uf, tribunais, links_tribunais,
                tipo_informacao, nm['ANIVERSARIO'], base["texto"], nm['INDICADOR_PPE'], pep_rel,
                base["url"], orgao, emp_rel, cnpj_emp_rel,
                relacionamento, dt_ini, dt_fim, dt_car
            ))

        inserted = 0
        for i in range(0, len(batch), chunk_size):
            sub = batch[i:i+chunk_size]
            try:
                cursor.executemany(insert_sql, sub)
                conn.commit()
                inserted += len(sub)
            except Exception as e:
                logger.warning(f"[aux] falha no sublote {i}-{i+len(sub)}: {e} (fallback por linha)")
                conn.rollback()
                # fallback resiliente: transações curtíssimas por linha
                conn.autocommit = True
                for vals in sub:
                    try:
                        cursor.execute(insert_sql, vals)
                        inserted += 1
                    except Exception as ee:
                        logger.error(f"[aux] falhou 1 linha no fallback: {ee}")
                conn.autocommit = False

        # marca a notícia
        try:
            cursor.execute(
                "UPDATE TB_NOTICIA_RASPADA SET STATUS=%s, DT_TRANSFERENCIA=NOW() WHERE ID=%s",
                ("203-PUBLISHED" if inserted > 0 else "205-TRANSFERED", news.get("ID"))
            )
            conn.commit()
        except Exception as e:
            logger.error(f"[aux] falhou UPDATE status da notícia {news.get('ID')}: {e}")
            conn.rollback()

        return inserted, inserted > 0

    finally:
        try: cursor.close()
        except: pass
        try: conn.close()
        except: pass

def run_transfer(date_directory: str | None, category: str | None, logger):
    # data default = hoje (YYYYMMDD)
    date_dir = date_directory or datetime.now().strftime("%Y%m%d")
    logger.info(f"Iniciando transferência (modo por notícia) DATE_DIRECTORY={date_dir} CATEGORY={category}")

    # filtro por REG_NOTICIA LIKE (categoria + data)
    reg_like = None
    if category:
        try:
            abrev, cat_prefix, full_name = normalize_category(category)
            if not cat_prefix:
                raise ValueError(f"Prefixo não mapeado para categoria: {category}")
            reg_like = f"{cat_prefix}{date_dir}%"
            logger.info(f"Filtro: REG_NOTICIA LIKE '{reg_like}' ({full_name}/{abrev})")
        except ValueError as e:
            logger.error(str(e))
            return {"date": date_dir, "moved": 0, "failed": 0, "inserted": 0,
                    "published": [], "not_published": [], "error": str(e)}

    # 1) carrega todas as 201-APPROVED (com filtro opcional)
    registros = fetch_registros(logger, reg_like=reg_like)
    logger.info(f"Registros para processar: {len(registros)}")
    if not registros:
        return {"date": date_dir, "moved": 0, "failed": 0, "inserted": 0,
                "published": [], "not_published": [], "scheduled": []}

    moved, failed = [], []
    scheduled = []

    # 2) processa CADA notícia: transferir ➜ agendar inserção de nomes
    for reg in registros:
        lp, rd = construir_caminhos(reg, date_dir)

        # 2.1 transferir essa notícia
        if transferir_arquivo(lp, rd, reg['ID'], logger):
            moved.append(reg['REG_NOTICIA'])
        else:
            failed.append(reg['REG_NOTICIA'])
            continue

        # 2.2 agendar inserção na fila dedicada (assíncrono)
        async_result = insert_names_task.delay(reg['ID'])
        scheduled.append({"news_id": reg["ID"], "task_id": async_result.id})

    summary = {
        "date": date_dir,
        "moved": len(moved),
        "failed": len(failed),
        "inserted": 0,             # inserção é assíncrona agora
        "published": [],           # será marcado pelo task de insert
        "not_published": [],       # idem
        "scheduled": scheduled,    # lista de tasks enfileiradas
    }
    logger.info(f"Resumo: {summary}")
    return summary

@celery_app.task(
    name="dtecflex.insert_names",
    queue="aux-insert",
    acks_late=True,
    soft_time_limit=5*60,
    time_limit=5*60,
    max_retries=0,
)
def insert_names_task(news_id: int):
    # Busca news + names "frescos" no momento da execução
    news = load_news(news_id)
    # logger pode ser global/importado; se não tiver, substitua por um stub
    from src.dtecflex_extract_api.config.celery import logger  # ajuste se necessário
    names = fetch_nomes_por_noticia(news_id, logger)
    return insert_names_to_aux_for_news(news, names, logger)
