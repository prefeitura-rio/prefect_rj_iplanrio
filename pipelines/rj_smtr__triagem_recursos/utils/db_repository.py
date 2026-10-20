"""
Camada de repositório e persistência transacional para o banco de triagem.
"""

import json
import logging
from typing import Any, Dict, List, Optional
import psycopg2
from psycopg2.extras import RealDictCursor, execute_values

from ..constants import (
    TABELA_FILA,
    TABELA_PREDICOES,
    TABELA_PROCESSOS,
    StatusProcessamento,
)

logger = logging.getLogger(__name__)


def buscar_processos_para_triagem(
    db_url: str,
    modo: str = "INCREMENTAL",
    limite: int = 300,
    data_inicio: Optional[str] = None,
    data_fim: Optional[str] = None,
) -> List[Dict[str, Any]]:
    origem_alvo = "MENSAL" if modo.upper() == "INCREMENTAL" else "PASSIVO"

    query = f"""
        SELECT 
            p.processo_id,
            p.numero_processo_sei,
            p.numero_auto_infracao,
            p.nome_arquivo_origem,
            p.data_autuacao
        FROM {TABELA_PROCESSOS} p
        INNER JOIN {TABELA_FILA} f ON p.processo_id = f.processo_id
        WHERE f.status = %s
          AND p.origem_lote = %s
          AND (%s::date IS NULL OR p.data_autuacao >= %s::date)
          AND (%s::date IS NULL OR p.data_autuacao <= %s::date)
        ORDER BY f.prioridade_ordenacao DESC, p.data_autuacao ASC
        LIMIT %s;
    """

    conn = psycopg2.connect(db_url)
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(
                query,
                (
                    StatusProcessamento.PENDENTE_IA.value,
                    origem_alvo,
                    data_inicio,
                    data_inicio,
                    data_fim,
                    data_fim,
                    limite,
                ),
            )
            return [dict(r) for r in cursor.fetchall()]
    finally:
        conn.close()


def persistir_lote_resultados_ia(
    db_url: str, resultados: List[Dict[str, Any]]
) -> None:
    if not resultados:
        return

    query_predicoes = f"""
        INSERT INTO {TABELA_PREDICOES} (
            processo_id,
            n1_classe_predita,
            n1_encaminhamento,
            n1_cot_justificativa,
            n1_lauda_evidencia,
            n1_frase_citada,
            n1_tipo_citacao,
            feat_fuzz_ratio,
            feat_token_containment,
            feat_exact_match,
            feat_adversarial_density,
            feat_sim_centroid_pred,
            feat_mahalanobis_dist,
            feat_is_manuscrito,
            feat_tesseract_mean_conf,
            probabilidade_calibrada_n1,
            conjunto_conformal,
            tamanho_conjunto_conformal,
            estagio2_executado,
            n2_classe_predita,
            n2_cot_justificativa,
            n2_delta_cot_dispersao,
            n2_log_odds_atualizado,
            probabilidade_final,
            rota_execucao,
            faixa_operacional,
            custo_estimado_usd,
            tempo_execucao_s
        ) VALUES %s;
    """

    valores = []
    processos_ids = []

    for r in resultados:
        processos_ids.append(r["processo_id"])
        sinais = r["sinais_epistemicos"]
        valores.append((
            r["processo_id"],
            r["n1_classe_predita"],
            r["n1_encaminhamento"],
            r["n1_cot_justificativa"],
            r["n1_lauda_evidencia"],
            r.get("n1_frase_citada", ""),
            r.get("n1_tipo_citacao", "FATO_DECLARADO"),
            float(sinais.get("fuzz_ratio", 0.0)),
            float(sinais.get("token_containment", 0.0)),
            float(sinais.get("exact_match", 0.0)),
            float(sinais.get("adversarial_density", 0.0)),
            float(sinais.get("sim_centroid_pred", 0.0)),
            float(sinais.get("mahalanobis_dist", 0.0))
            if sinais.get("mahalanobis_dist") is not None
            else None,
            float(sinais.get("is_manuscrito", 0.0)),
            float(sinais.get("tesseract_mean_conf", 0.0)),
            float(r["probabilidade_calibrada"]),
            r["conjunto_conformal"],
            int(r["tamanho_conjunto_conformal"]),
            bool(r["estagio2_executado"]),
            r.get("n2_classe_predita"),
            r.get("n2_cot_justificativa"),
            float(r["n2_delta_cot_dispersao"])
            if r.get("n2_delta_cot_dispersao") is not None
            else None,
            float(r["n2_log_odds_atualizado"])
            if r.get("n2_log_odds_atualizado") is not None
            else None,
            float(r["probabilidade_final"]),
            r["rota_execucao"],
            r["faixa_operacional"],
            float(r.get("custo_estimado_usd", 0.0)),
            float(r.get("tempo_execucao_s", 0.0)),
        ))

    query_update_fila = f"""
        UPDATE {TABELA_FILA}
        SET 
            status = %s,
            atualizado_em = CURRENT_TIMESTAMP
        WHERE processo_id = ANY(%s::uuid[]);
    """

    conn = psycopg2.connect(db_url)
    try:
        with conn.cursor() as cursor:
            execute_values(cursor, query_predicoes, valores)
            cursor.execute(
                query_update_fila,
                (StatusProcessamento.CONCLUIDO_IA.value, processos_ids),
            )
        conn.commit()
        logger.info(
            f"Gravados {len(resultados)} resultados no banco de triagem com"
            " sucesso."
        )
    except Exception as e:
        conn.rollback()
        logger.error(f"Erro ao persistir lote: {e}")
        raise
    finally:
        conn.close()