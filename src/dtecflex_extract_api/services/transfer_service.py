import os
import glob
import subprocess
from collections import defaultdict
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Tuple
import mysql.connector

from src.dtecflex_extract_api.config.celery import settings
ProgressCb = Optional[Callable[[int, int, str, dict | None], None]]
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

    print('result:::', result)

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

def insert_names_to_aux(noticias, logger):
    published_news     = []
    not_published_news = []
    total_inserted     = 0

    try:
        # ✅ usa o mesmo banco da aplicação
        conn   = _db_conn()
        cursor = conn.cursor()

        insert_query = """
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
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s, NOW(),
                %s, %s, %s, %s, NOW(),
                %s, %s, %s, %s, %s,
                %s, NOW(), %s, %s, %s,
                %s, %s, %s, %s
            )
        """

        dtec = existem_processos = origem_uf = tribunais = links_tribunais = None
        pep_relacionado = orgao = empresa_relacionada = cnpj_empresa_relacionada = None
        relacionamento = data_inicio_mandato = data_fim_mandato = data_carencia = None

        for news in noticias:
            categoria = news.get("CATEGORIA")
            tipo_suspeita, tipo_informacao = CATEGORY_MAPPING.get(categoria, (None, None))

            news_id      = news.get("ID")
            titulo       = news.get("TITULO")
            data_noticia = news.get("DATA_PUBLICACAO")
            fonte        = news.get("FONTE")
            regiao       = news.get("REGIAO")
            estado       = news.get("UF")
            reg_noticia  = news.get("REG_NOTICIA")
            texto        = news.get("TEXTO_NOTICIA")

            names_inserted = 0
            for name in news.get("NAMES", []):
                values = (
                    name['NOME'], name['CPF'], name['NOME_CPF'], name['APELIDO'], dtec,
                    name['SEXO'], name['PESSOA'], name['IDADE'], name['ATIVIDADE'], name['ENVOLVIMENTO'],
                    tipo_suspeita, name['OPERACAO'], titulo, data_noticia, fonte,
                    regiao, estado, reg_noticia, name['FLG_PESSOA_PUBLICA'],
                    existem_processos, origem_uf, tribunais, links_tribunais,
                    tipo_informacao, name['ANIVERSARIO'], texto, name['INDICADOR_PPE'],
                    pep_relacionado, news.get("URL"), orgao, empresa_relacionada,
                    cnpj_empresa_relacionada, relacionamento, data_inicio_mandato,
                    data_fim_mandato, data_carencia
                )
                try:
                    cursor.execute(insert_query, values)
                    total_inserted += 1
                    names_inserted += 1
                    logger.info(f"Nome '{name['NOME']}' inserido para notícia {news_id}")
                except Exception as err:
                    logger.error(f"Erro ao inserir '{name['NOME']}' (notícia {news_id}): {err}")

            if names_inserted > 0:
                try:
                    # ✅ publica a notícia
                    cursor.execute(
                        """
                        UPDATE TB_NOTICIA_RASPADA
                           SET STATUS = %s,
                               DT_TRANSFERENCIA = NOW()
                         WHERE ID = %s
                           AND STATUS <> %s
                        """,
                        ("203-PUBLISHED", news_id, "203-PUBLISHED")
                    )
                    published_news.append(news_id)
                except Exception as err:
                    logger.error(f"Erro ao atualizar notícia {news_id} para 203-PUBLISHED: {err}")
                    not_published_news.append(news_id)
            else:
                not_published_news.append(news_id)

        conn.commit()
        # ✅ corrige o log
        logger.info(f"Total de nomes inseridos na Auxiliar: {total_inserted}")

    except Exception as err:
        logger.error(f"Erro ao inserir na Auxiliar: {err}")
    finally:
        try:
            cursor.close()
        except Exception:
            pass
        try:
            if conn.is_connected():
                conn.close()
        except Exception:
            pass

    return published_news, not_published_news

# def insert_names_to_aux(noticias, logger):
#     published_news     = []
#     not_published_news = []
#     total_inserted     = 0

#     DB_CONFIG = {
#         'user':     'dtecflex',
#         'password': 'softon1245',
#         # 'host':     'dtec-flex.com.br',
#         'host':     '10.10.10.24',
#         'port':     '3306',
#         'database': 'dtecflex'
#     }

#     try:
#         conn   = mysql.connector.connect(**DB_CONFIG)
#         cursor = conn.cursor()

#         insert_query = """
#             INSERT INTO Auxiliar (
#                 NOME, CPF, NOME_CPF, APELIDO, DTEC,
#                 SEXO, PESSOA, IDADE, ATIVIDADE, ENVOLVIMENTO,
#                 TIPO_SUSPEITA, OPERACAO, TITULO, DATA_NOTICIA, FONTE_NOTICIA,
#                 REGIAO, ESTADO, REGISTRO_NOTICIA, FLG_PESSOA_PUBLICA, DATA_GRAVACAO,
#                 EXISTEM_PROCESSOS, ORIGEM_UF, TRIBUNAIS, LINKS_TRIBUNAIS, DATA_PESQUISA,
#                 TIPO_INFORMACAO, ANIVERSARIO, CITACOES_NA_MIDIA, INDICADOR_PPE, PEP_RELACIONADO,
#                 LINK_NOTICIA, DATA_ATUALIZACAO, ORGAO, EMPRESA_RELACIONADA, CNPJ_EMPRESA_RELACIONADA,
#                 RELACIONAMENTO, DATA_INICIO_MANDATO, DATA_FIM_MANDATO, DATA_CARENCIA
#             ) VALUES (
#                 %s, %s, %s, %s, %s,
#                 %s, %s, %s, %s, %s,
#                 %s, %s, %s, %s, %s,
#                 %s, %s, %s, %s, NOW(),
#                 %s, %s, %s, %s, NOW(),
#                 %s, %s, %s, %s, %s,
#                 %s, NOW(), %s, %s, %s,
#                 %s, %s, %s, %s
#             )
#         """

#         dtec                      = None
#         existem_processos         = None
#         origem_uf                 = None
#         tribunais                 = None
#         links_tribunais           = None
#         pep_relacionado           = None
#         orgao                     = None
#         empresa_relacionada       = None
#         cnpj_empresa_relacionada  = None
#         relacionamento            = None
#         data_inicio_mandato       = None
#         data_fim_mandato          = None
#         data_carencia             = None

#         for news in noticias:
#             categoria            = news.get("CATEGORIA")
#             tipo_suspeita, tipo_informacao = CATEGORY_MAPPING.get(categoria, (None, None))

#             news_id    = news.get("ID")
#             titulo     = news.get("TITULO")
#             data_noticia = news.get("DATA_PUBLICACAO")
#             fonte      = news.get("FONTE")
#             regiao     = news.get("REGIAO")
#             estado     = news.get("UF")
#             reg_noticia = news.get("REG_NOTICIA")
#             texto      = news.get("TEXTO_NOTICIA")

#             names_inserted = 0
#             for name in news.get("NAMES", []):
#                 values = (
#                     name['NOME'],
#                     name['CPF'],
#                     name['NOME_CPF'],
#                     name['APELIDO'],
#                     dtec,
#                     name['SEXO'],
#                     name['PESSOA'],
#                     name['IDADE'],
#                     name['ATIVIDADE'],
#                     name['ENVOLVIMENTO'],
#                     tipo_suspeita,
#                     name['OPERACAO'],
#                     titulo,
#                     data_noticia,
#                     fonte,
#                     regiao,
#                     estado,
#                     reg_noticia,
#                     name['FLG_PESSOA_PUBLICA'],
#                     existem_processos,
#                     origem_uf,
#                     tribunais,
#                     links_tribunais,
#                     tipo_informacao,
#                     name['ANIVERSARIO'],
#                     texto,
#                     name['INDICADOR_PPE'],
#                     pep_relacionado,
#                     news.get("URL"),
#                     orgao,
#                     empresa_relacionada,
#                     cnpj_empresa_relacionada,
#                     relacionamento,
#                     data_inicio_mandato,
#                     data_fim_mandato,
#                     data_carencia
#                 )
#                 try:
#                     cursor.execute(insert_query, values)
#                     total_inserted += 1
#                     names_inserted += 1
#                     logger.info(f"Nome '{name['NOME']}' inserido para notícia {news_id}")
#                 except Exception as err:
#                     logger.error(f"Erro ao inserir '{name['NOME']}' (notícia {news_id}): {err}")

#             if names_inserted > 0:
#                 try:
#                     cursor.execute(
#                         "UPDATE TB_NOTICIA_RASPADA SET STATUS=%s, DT_TRANSFERENCIA=NOW() WHERE ID=%s",
#                         ("203-PUBLISHED", news_id)
#                     )
#                     published_news.append(news_id)
#                 except Exception as err:
#                     logger.error(f"Erro ao atualizar notícia {news_id} para 203-PUBLISHED: {err}")
#                     not_published_news.append(news_id)
#             else:
#                 not_published_news.append(news_id)

#         conn.commit()
#         logger.info("Total de nomes inseridos na Auxiliar: {total_inserted}")

#     except Exception as err:
#         logger.error(f"Erro ao inserir na Auxiliar: {err}")
#     finally:
#         if 'cursor' in locals() and cursor:
#             cursor.close()
#         if 'conn' in locals() and conn.is_connected():
#             conn.close()

#     return published_news, not_published_news

# def run_transfer(date_directory: str | None, category: str | None, logger):
#     date_dir = date_directory or datetime.now().strftime("%Y%m%d")
#     logger.info(f"Iniciando transferência (modo por notícia) DATE_DIRECTORY={date_dir} CATEGORY={category}")

#     reg_like = None
#     if category:
#         try:
#             abrev, cat_prefix, full_name = normalize_category(category)
#             if not cat_prefix:
#                 raise ValueError(f"Prefixo não mapeado para categoria: {category}")
#             reg_like = f"{cat_prefix}{date_dir}%"  # ex.: "C20250817%"
#             logger.info(f"Filtro: REG_NOTICIA LIKE '{reg_like}' ({full_name}/{abrev})")
#         except ValueError as e:
#             logger.error(str(e))
#             return {"date": date_dir, "moved": 0, "failed": 0, "inserted": 0,
#                     "published": [], "not_published": [], "error": str(e)}

#     registros = fetch_registros(logger, reg_like=reg_like)
#     if not registros:
#         logger.info("Nenhum registro para processar com os filtros informados.")
#         return {"date": date_dir, "moved": 0, "failed": 0, "inserted": 0,
#                 "published": [], "not_published": []}

#     moved, failed = [], []
#     published, not_published = [], []
#     total_inserted = 0

#     for reg in registros:
#         lp, rd = construir_caminhos(reg, date_dir)

#         if transferir_arquivo(lp, rd, reg['ID'], logger):
#             moved.append(reg['REG_NOTICIA'])
#         else:
#             failed.append(reg['REG_NOTICIA'])
#             continue

#         names = fetch_nomes_por_noticia(reg['ID'], logger)

#         news = {
#             'ID': reg['ID'],
#             'URL': reg.get('URL'),
#             'FONTE': reg.get('FONTE'),
#             'DATA_PUBLICACAO': reg.get('DATA_PUBLICACAO'),
#             'CATEGORIA': reg.get('CATEGORIA'),
#             'REG_NOTICIA': reg.get('REG_NOTICIA'),
#             'TEXTO_NOTICIA': reg.get('TEXTO_NOTICIA'),
#             'UF': reg.get('UF'),
#             'REGIAO': reg.get('REGIAO'),
#             'OPERACAO': reg.get('OPERACAO'),
#             'TITULO': reg.get('TITULO'),
#             'NAMES': names,  # << importante!
#         }

#         published_news, not_published_news = insert_names_to_aux([news], logger)

#         if reg['ID'] in published_news:
#             published.append(reg['ID'])
#             total_inserted += len(names)
#         else:
#             not_published.append(reg['ID'])

#     summary = {
#         "date": date_dir,
#         "moved": len(moved),
#         "failed": len(failed),
#         "inserted": total_inserted,
#         "published": published,
#         "not_published": not_published,
#     }
#     logger.info(f"Resumo: {summary}")
#     return summary

def run_transfer(date_directory: str | None, category: str | None, logger, progress_cb: ProgressCb = None):
    date_dir = date_directory or datetime.now().strftime("%Y%m%d")

    # >>> define reg_like sempre
    reg_like = None
    if category:
        try:
            abrev, cat_prefix, full_name = normalize_category(category)
            if not cat_prefix:
                raise ValueError(f"Prefixo não mapeado para categoria: {category}")
            reg_like = f"{cat_prefix}{date_dir}%"   # ex.: "C20250904%"
            logger.info(f"Filtro: REG_NOTICIA LIKE '{reg_like}' ({full_name}/{abrev})")
        except ValueError as e:
            logger.error(str(e))
            if progress_cb:
                progress_cb(0, 1, "FAILED", {"error": str(e)})
            return {"date": date_dir, "moved": 0, "failed": 0, "inserted": 0,
                    "published": [], "not_published": [], "error": str(e)}
    # <<<

    # ... (seu código intacto até obter "registros")
    registros = fetch_registros(logger, reg_like=reg_like)
    total = len(registros)
    if progress_cb:
        progress_cb(0, total or 1, "START", None)
    if not registros:
        return {"date": date_dir, "moved": 0, "failed": 0, "inserted": 0,
                "published": [], "not_published": []}

    moved, failed = [], []
    published, not_published = [], []
    total_inserted = 0

    for idx, reg in enumerate(registros, start=1):
        # RSYNC
        lp, rd = construir_caminhos(reg, date_dir)
        ok = transferir_arquivo(lp, rd, reg['ID'], logger)
        if ok: moved.append(reg['REG_NOTICIA'])
        else:  failed.append(reg['REG_NOTICIA'])

        if progress_cb:
            progress_cb(idx, total, "RSYNC", {"last": reg['REG_NOTICIA']})

        if not ok:
            continue

        # NOMES → Auxiliar
        names = fetch_nomes_por_noticia(reg['ID'], logger)
        news = {
            'ID': reg['ID'], 'URL': reg.get('URL'), 'FONTE': reg.get('FONTE'),
            'DATA_PUBLICACAO': reg.get('DATA_PUBLICACAO'), 'CATEGORIA': reg.get('CATEGORIA'),
            'REG_NOTICIA': reg.get('REG_NOTICIA'), 'TEXTO_NOTICIA': reg.get('TEXTO_NOTICIA'),
            'UF': reg.get('UF'), 'REGIAO': reg.get('REGIAO'), 'OPERACAO': reg.get('OPERACAO'),
            'TITULO': reg.get('TITULO'), 'NAMES': names,
        }

        published_news, not_published_news = insert_names_to_aux([news], logger)
        if reg['ID'] in published_news:
            published.append(reg['ID'])
            total_inserted += len(names)
        else:
            not_published.append(reg['ID'])

        if progress_cb:
            progress_cb(idx, total, "AUX_INSERT", {"inserted_for_news": len(names)})

    summary = {
        "date": date_dir, "moved": len(moved), "failed": len(failed),
        "inserted": total_inserted, "published": published, "not_published": not_published,
    }
    if progress_cb:
        progress_cb(total, total or 1, "SUMMARY", summary)
    logger.info(f"Resumo: {summary}")
    return summary