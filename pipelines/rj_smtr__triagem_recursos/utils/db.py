"""
Módulo de conexão e persistência de dados no Cloud SQL (PostgreSQL).
Implementa pool de conexões e operações transacionais para a pipeline Prefect.
"""

from contextlib import contextmanager
import json
import logging
from typing import Any, Dict, List, Optional
import psycopg2
from psycopg2.extras import RealDictCursor, execute_values
from psycopg2.pool import ThreadedConnectionPool

from ..constants import (
    CLOUD_SQL_DB_URL,
    DB_CONNECT_TIMEOUT,
    DB_POOL_MAX_CONN,
    DB_POOL_MIN_CONN,
)

logger = logging.getLogger(__name__)

_POOL: Optional[ThreadedConnectionPool] = None


def inicializar_pool_conexoes() -> ThreadedConnectionPool:
    """Inicializa o pool de conexões thread-safe para execução paralela de tasks."""
    global _POOL
    if _POOL is None or _POOL.closed:
        _POOL = ThreadedConnectionPool(
            minconn=DB_POOL_MIN_CONN,
            maxconn=DB_POOL_MAX_CONN,
            dsn=CLOUD_SQL_DB_URL,
            connect_timeout=DB_CONNECT_TIMEOUT,
        )
    return _POOL


def encerrar_pool_conexoes() -> None:
    """Encerra o pool de conexões liberando recursos."""
    global _POOL
    if _POOL is not None and not _POOL.closed:
        _POOL.closeall()
        _POOL = None


@contextmanager
def obter_conexao():
    """Gerenciador de contexto para obter e devolver conexões ao pool."""
    pool = inicializar_pool_conexoes()
    conn = pool.getconn()
    try:
        yield conn
    finally:
        pool.putconn(conn)


def buscar_processos_pendentes(
    origem: str = "MENSAL",
    limite: int = 300,
    data_inicio: Optional[str] = None,
    data_fim: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Recupera processos da fila com status 'PENDENTE_IA'.
    Suporta filtragem por lote (MENSAL ou PASSIVO) e recorte temporal.
    """
    query = """
        SELECT 
            p.processo_id,
            p.numero_processo_sei,
            p.numero_auto_infracao,
            p.gcs_uri_pdf,
            p.data_infracao,
            f.fila_id,
            f.prioridade
        FROM fila_trabalho f
        JOIN processos_administrativos p ON f.processo_id = p.processo_id
        WHERE f.status = 'PENDENTE_IA'
          AND p.origem_lote = %s
          AND (%s::date IS NULL OR p.data_infracao >= %s::date)
          AND (%s::date IS NULL OR p.data_infracao <= %s::date)
        ORDER BY f.prioridade DESC, p.data_infracao ASC
        LIMIT %s;
    """
    with obter_conexao() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query, (origem, data_inicio, data_inicio, data_fim, data_fim, limite))
            registros = cursor.fetchall()
            return [dict(r) for r in registros]


def persistir_resultados_lote(resultados: List[Dict[str, Any]]) -> None:
    """
    Persiste os resultados analíticos da IA e atualiza o status na fila de trabalho
    dentro de uma transação atômica.
    """
    if not resultados:
        return

    query_predicoes = """
        INSERT INTO predicoes_pipeline (
            processo_id,
            n1_classe_predita,
            n1_encaminhamento,
            n1_cot_justificativa,
            n1_lauda_evidencia,
            n1_frase_citada,
            n1_tipo_citacao,
            sinais_epistemicos,
            p_cal_n1,
            conjunto_conformal,
            tamanho_conformal,
            estagio2_executado,
            n2_delta_cot,
            probabilidade_final,
            rota_execucao,
            faixa_operacional,
            tempo_processamento_s,
            custo_estimado_usd
        ) VALUES %s;
    """

    query_atualiza_fila = """
        UPDATE fila_trabalho AS f
        SET 
            status = 'CONCLUIDO_IA',
            atualizado_em = CURRENT_TIMESTAMP
        FROM (VALUES %s) AS v(processo_id)
        WHERE f.processo_id = v.processo_id::uuid;
    """

    valores_predicoes = []
    ids_atualizar = []

    for r in resultados:
        valores_predicoes.append((
            r["processo_id"],
            r["n1_classe_predita"],
            r["n1_encaminhamento"],
            r["n1_cot_justificativa"],
            r["n1_lauda_evidencia"],
            r["n1_frase_citada"],
            r["n1_tipo_citacao"],
            json.dumps(r["sinais_epistemicos"]),
            r["p_cal_n1"],
            r["conjunto_conformal"],
            r["tamanho_conformal"],
            r["estagio2_executado"],
            r.get("n2_delta_cot"),
            r["probabilidade_final"],
            r["rota_execucao"],
            r["faixa_operacional"],
            r["tempo_processamento_s"],
            r["custo_estimado_usd"],
        ))
        ids_atualizar.append((r["processo_id"],))

    with obter_conexao() as conn:
        try:
            with conn.cursor() as cursor:
                # 1. Inserção em bloco dos resultados analíticos
                execute_values(cursor, query_predicoes, valores_predicoes)
                
                # 2. Atualização em bloco do status na fila
                execute_values(cursor, query_atualiza_fila, ids_atualizar)
                
            conn.commit()
            logger.info("Lote de %d resultados persistido no Cloud SQL com sucesso.", len(resultados))
        except Exception as e:
            conn.rollback()
            logger.error("Erro na transação de persistência em lote: %s", str(e))
            raise e


def registrar_falha_processo(processo_id: str, mensagem_erro: str) -> None:
    """Registra falha de processamento na fila para inspeção operacional."""
    query = """
        UPDATE fila_trabalho
        SET 
            status = 'FALHA_PROCESSAMENTO',
            tentativas_processamento = tentativas_processamento + 1,
            erro_mensagem = %s,
            atualizado_em = CURRENT_TIMESTAMP
        WHERE processo_id = %s;
    """
    with obter_conexao() as conn:
        try:
            with conn.cursor() as cursor:
                cursor.execute(query, (mensagem_erro[:1000], processo_id))
            conn.commit()
        except Exception as e:
            conn.rollback()
            logger.error("Erro ao registrar falha do processo %s: %s", processo_id, str(e))