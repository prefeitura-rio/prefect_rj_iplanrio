"""
Camada de repositório e persistência transacional para o banco de triagem.
"""

import json
import logging
from typing import Any, Dict, List, Optional
import numpy as np
import psycopg2
from psycopg2.extras import RealDictCursor, execute_values

from ..constants import (
    TABELA_CALIBRACAO,
    TABELA_FILA,
    TABELA_PREDICOES,
    TABELA_PROCESSOS,
    StatusProcessamento,
)

logger = logging.getLogger(__name__)


def _val_ou_none(v: Any) -> Optional[float]:
    """Converte np.nan ou None para None (SQL NULL), senão float."""
    if v is None:
        return None
    try:
        val = float(v)
        return None if np.isnan(val) else val
    except (ValueError, TypeError):
        return None


def buscar_configuracao_calibracao_ativa(db_url: str) -> Dict[str, Any]:
    """Recupera a configuração ativa de calibração persistida no PostgreSQL.

    :param db_url: URL de conexão do PostgreSQL (local ou Cloud SQL).
    :return: Dicionário com parâmetros puros, quantis e o modelo textual do LightGBM.
    :raises RuntimeError: Caso nenhuma configuração ativa seja encontrada.
    """
    query = f"""
        SELECT 
            calibracao_id,
            versao_modelo,
            total_amostras_treino,
            brier_score_treino,
            log_loss_treino,
            medianas_imputacao,
            medias_scaler,
            desvios_scaler,
            quantis_conformais_cccp,
            quantil_pool_fallback,
            limiar_crc_grounding,
            alpha_conformal,
            estatisticas_classes,
            lightgbm_model_text
        FROM {TABELA_CALIBRACAO}
        WHERE ativo = TRUE
        LIMIT 1;
    """
    conn = psycopg2.connect(db_url)
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query)
            row = cursor.fetchone()
            if not row:
                raise RuntimeError(
                    "Nenhuma configuração de calibração ativa (ativo = TRUE) encontrada no banco. "
                    "Execute o script de seed para inicializar os parâmetros."
                )

            res = dict(row)
            for campo_json in [
                "medianas_imputacao",
                "medias_scaler",
                "desvios_scaler",
                "quantis_conformais_cccp",
                "estatisticas_classes",
            ]:
                if isinstance(res.get(campo_json), str):
                    res[campo_json] = json.loads(res[campo_json])
            return res
    finally:
        conn.close()


def filtrar_processos_ja_triados(
    db_url: str, numeros_processos_sei: List[str]
) -> List[str]:
    """Retorna a lista de números SEI que já foram triados (status CONCLUIDO_IA)."""
    if not numeros_processos_sei:
        return []

    query = f"""
        SELECT p.numero_processo_sei
        FROM {TABELA_PROCESSOS} p
        INNER JOIN {TABELA_FILA} f ON p.processo_id = f.processo_id
        WHERE p.numero_processo_sei = ANY(%s)
          AND f.status = %s;
    """
    conn = psycopg2.connect(db_url)
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                query,
                (numeros_processos_sei, StatusProcessamento.CONCLUIDO_IA.value),
            )
            return [row[0] for row in cursor.fetchall()]
    finally:
        conn.close()


def buscar_processos_para_triagem(
    db_url: str,
    modo: str = "INCREMENTAL",
    limite: int = 300,
    data_inicio: Optional[str] = None,
    data_fim: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Busca processos na fila de trabalho com status PENDENTE_IA."""
    origem_alvo = "MENSAL" if modo.upper() == "INCREMENTAL" else "PASSIVO"

    query = f"""
        SELECT 
            p.processo_id,
            p.numero_processo_sei,
            p.id_procedimento_sei,
            p.id_documento_sei,
            p.numero_auto_infracao,
            p.nome_arquivo_origem,
            p.data_autuacao
        FROM {TABELA_PROCESSOS} p
        INNER JOIN {TABELA_FILA} f ON p.processo_id = f.processo_id
        WHERE f.status = %s
          AND (p.origem_lote = %s OR p.origem_lote = 'SEI_JARI')
          AND (%s::date IS NULL OR p.data_autuacao >= %s::date)
          AND (%s::date IS NULL OR p.data_autuacao <= %s::date)
        ORDER BY p.criado_em ASC
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
    """Persiste as predições, sinais epistêmicos e auditoria de sistema no PostgreSQL."""
    if not resultados:
        return

    query_predicoes = f"""
        INSERT INTO {TABELA_PREDICOES} (
            processo_id,
            calibracao_id,
            modelo_llm,
            versao_prompt,
            id_documento_sei,
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
            auditoria_prefixo_auto,
            auditoria_codigo_status,
            auditoria_fase_descricao,
            auditoria_possui_espelho,
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
            tokens_prompt_total,
            tokens_completion,
            tokens_cached,
            custo_real_usd,
            custo_sem_cache_usd,
            economia_cache_usd,
            custo_estimado_usd,
            tempo_execucao_s
        ) VALUES %s;
    """

    valores = []
    processos_ids = []

    for r in resultados:
        processos_ids.append(r["processo_id"])
        sinais = r.get("sinais_epistemicos", {})
        auditoria = r.get("auditoria_sistema", {})

        valores.append((
            r["processo_id"],
            r.get("calibracao_id"),
            r.get("modelo_llm", "vertex/gemini-3.7-flash"),
            r.get("versao_prompt", "v4_baseline"),
            r.get("id_documento_sei"),
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
            _val_ou_none(sinais.get("mahalanobis_dist")),
            float(sinais.get("is_manuscrito", 0.0)),
            float(sinais.get("tesseract_mean_conf", 0.0)),
            auditoria.get("prefixo_auto"),
            auditoria.get("codigo_status"),
            auditoria.get("fase_descricao"),
            bool(auditoria.get("possui_espelho_nada_consta", True)),
            float(r["probabilidade_calibrada"]),
            r["conjunto_conformal"],
            int(r["tamanho_conjunto_conformal"]),
            bool(r["estagio2_executado"]),
            r.get("n2_classe_predita"),
            r.get("n2_cot_justificativa"),
            _val_ou_none(r.get("n2_delta_cot_dispersao")),
            _val_ou_none(r.get("n2_log_odds_atualizado")),
            float(r["probabilidade_final"]),
            r["rota_execucao"],
            r["faixa_operacional"],
            int(r.get("tokens_prompt_total", 0)),
            int(r.get("tokens_completion", 0)),
            int(r.get("tokens_cached", 0)),
            float(r.get("custo_real_usd", r.get("custo_estimado_usd", 0.0))),
            float(r.get("custo_sem_cache_usd", 0.0)),
            float(r.get("economia_cache_usd", 0.0)),
            float(r.get("custo_real_usd", r.get("custo_estimado_usd", 0.0))),
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
            "Gravados %d resultados no banco de triagem com sucesso.",
            len(resultados),
        )
    except Exception as e:
        conn.rollback()
        logger.error(f"Erro ao persistir lote: {e}")
        raise
    finally:
        conn.close()
