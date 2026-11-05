"""
Script de Ingestão e Carga Inicial (Seed) do Goldenset no Cloud SQL (PostgreSQL).

Lê os artefatos históricos consolidados (CSV calibrado + JSON detalhado),
normaliza as 8 features epistêmicas ativas e executa inserção transacional em lote
nas tabelas:
- processos_administrativos (origem_lote = 'GOLDENSET')
- predicoes_pipeline (inferência N=1, sinais epistêmicos, p_cal, conformal)
- fila_trabalho (status = 'CONCLUIDO_IA')
"""

import argparse
from datetime import datetime
import json
import logging
import os
from pathlib import Path
import re
import sys
from typing import Any, Dict, List, Optional, Tuple
import uuid

import numpy as np
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

# Força codificação UTF-8 no console Windows
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

FILE_PATH = Path(__file__).resolve()
PIPELINE_DIR = FILE_PATH.parent.parent
REPO_ROOT = PIPELINE_DIR.parent.parent

if str(REPO_ROOT) not in sys.path:
    sys.path.append(str(REPO_ROOT))

# Imports dos módulos reorganizados em utils/
from constants import (
    DATABASE_URL,
    FaixaOperacional,
    RotaExecucao,
    StatusProcessamento,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("seed_goldenset")


def normalizar_rotulo(rotulo: Any) -> str:
    """Normaliza strings e enums para o formato canônico snake_case."""
    if rotulo is None or pd.isna(rotulo):
        return ""
    texto = str(rotulo).strip().lower()
    if "." in texto:
        texto = texto.split(".")[-1]
    texto = re.sub(r"^(hipotese_legal|hipoteselegal|encaminhamento)[\s_:]*", "", texto)
    texto = re.sub(r"[\s\/\-]+", "_", texto)
    return re.sub(r"[^\w_]", "", texto).strip("_")


def categorizar_faixa(p: float) -> str:
    """Mapeia a probabilidade final para as três faixas operacionais."""
    if p >= 0.85:
        return FaixaOperacional.VERDE.value
    elif p >= 0.50:
        return FaixaOperacional.AMARELA.value
    return FaixaOperacional.VERMELHA.value


def carregar_arquivos_origem(
    caminho_csv: Path, caminho_json: Path
) -> Tuple[pd.DataFrame, Dict[str, Dict[str, Any]]]:
    """Carrega e indexa o CSV de calibração e o JSON de decisões."""
    if not caminho_csv.exists():
        raise FileNotFoundError(f"CSV de calibração não encontrado: {caminho_csv}")
    if not caminho_json.exists():
        raise FileNotFoundError(f"JSON de decisões não encontrado: {caminho_json}")

    logger.info(f"Lendo CSV de calibração: {caminho_csv.name}")
    df_cal = pd.read_csv(caminho_csv)

    logger.info(f"Lendo JSON de decisões: {caminho_json.name}")
    with open(caminho_json, "r", encoding="utf-8") as f:
        dados_json = json.load(f)

    dict_json = {item["arquivo"]: item for item in dados_json if "arquivo" in item}
    return df_cal, dict_json


def processar_registros_para_banco(
    df_cal: pd.DataFrame, dict_json: Dict[str, Dict[str, Any]]
) -> Tuple[List[Tuple], List[Tuple], List[Tuple]]:
    """Transforma os dados brutos nos registros formatados para o schema SQL."""
    registros_processos = []
    registros_predicoes = []
    registros_fila = []

    total_casos = len(df_cal)
    logger.info(f"Transformando e mapeando {total_casos} casos do Goldenset...")

    for _, row in df_cal.iterrows():
        nome_arq = str(row["arquivo"]).strip()
        item_json = dict_json.get(nome_arq, {})
        decisao = item_json.get("decisao_llm", {})
        auditoria_sis = decisao.get("auditoria_sistema", {})

        # Gera UUID determinístico baseado no nome do arquivo para garantir idempotência
        proc_uuid = str(uuid.uuid5(uuid.NAMESPACE_DNS, f"smtr.goldenset.{nome_arq}"))
        pred_uuid = str(uuid.uuid5(uuid.NAMESPACE_DNS, f"smtr.predicao.{nome_arq}"))
        fila_uuid = str(uuid.uuid5(uuid.NAMESPACE_DNS, f"smtr.fila.{nome_arq}"))

        # Extrai dados estruturados com fallback
        numero_sei = f"SEI-{nome_arq.replace('.pdf', '')[:28]}"
        auto_infr = str(row.get("auditoria_prefixo_auto", "NAO_IDENTIFICADO"))
        if auto_infr == "nan" or not auto_infr:
            auto_infr = None

        dt_infracao = str(row.get("auditoria_dt_infracao", "")).strip()
        data_autuacao_val = None
        if dt_infracao and dt_infracao not in ["NAO_IDENTIFICADO", "nan", ""]:
            try:
                # Tenta parsear datas comuns do espelho (DD/MM/AAAA)
                partes = dt_infracao.split("/")
                if len(partes) == 3:
                    data_autuacao_val = f"{partes[2]}-{partes[1]}-{partes[0]}"
            except Exception:
                data_autuacao_val = None

        # 1. Tabela: processos_administrativos
        registros_processos.append((
            proc_uuid,
            numero_sei,
            None,                       # id_procedimento_sei
            auto_infr,                  # numero_auto_infracao
            None,                       # placa_veiculo
            None,                       # cpf_cnpj_requerente
            data_autuacao_val,          # data_autuacao
            "GOLDENSET",                # origem_lote
            nome_arq,                   # nome_arquivo_origem
            None,                       # hash_sha256_documento
            datetime.utcnow(),          # criado_em
        ))

        # 2. Tabela: predicoes_pipeline
        p_cal_1 = float(row.get("p_calibrada", 0.50))
        p_final = p_cal_1  # No Goldenset inicial de calibração, N=1 define o baseline
        faixa = categorizar_faixa(p_final)
        rota = RotaExecucao.N1_DIRETO.value if p_final >= 0.85 else RotaExecucao.HITL_BAIXA_CONFIANCA.value

        # Trata as 8 features ativas (com fallback numérico)
        feat_fuzz = float(row.get("fuzz_ratio", 0.0))
        feat_token = float(row.get("token_containment", 0.0))
        feat_exact = float(row.get("exact_match", 0.0))
        feat_adv = float(row.get("adversarial_density", 0.0))
        feat_sim = float(row.get("sim_centroid_pred", 0.0))
        feat_mahal = float(row.get("mahalanobis_dist", np.nan))
        feat_mahal_val = feat_mahal if not np.isnan(feat_mahal) else None
        feat_manuscrito = float(row.get("is_manuscrito", 0.0))
        feat_tess = float(row.get("tesseract_mean_conf", 0.0))

        # Parsing de arrays e strings de listas
        raw_conformal = row.get("prediction_set_conformal", "[]")
        if isinstance(raw_conformal, list):
            set_conf_lista = raw_conformal
        elif isinstance(raw_conformal, str) and raw_conformal.startswith("["):
            try:
                set_conf_lista = json.loads(raw_conformal.replace("'", '"'))
            except Exception:
                set_conf_lista = [normalizar_rotulo(row.get("pred_hipotese", ""))]
        else:
            set_conf_lista = [normalizar_rotulo(row.get("pred_hipotese", ""))]

        cod_status_val = row.get("auditoria_status")
        cod_status_num = int(cod_status_val) if pd.notna(cod_status_val) and str(cod_status_val).isdigit() else None

        registros_predicoes.append((
            pred_uuid,
            proc_uuid,
            normalizar_rotulo(row.get("pred_hipotese", "")),
            normalizar_rotulo(row.get("pred_encaminhamento", "")),
            str(row.get("justificativa_analise", "")),
            int(row.get("pagina_evidencia", 0)) if pd.notna(row.get("pagina_evidencia")) else 0,
            str(row.get("frase_citada", "")),
            str(row.get("tipo_citacao", "FATO_DECLARADO")),
            feat_fuzz,
            feat_token,
            feat_exact,
            feat_adv,
            feat_sim,
            feat_mahal_val,
            feat_manuscrito,
            feat_tess,
            str(row.get("auditoria_prefixo_auto", "NAO_IDENTIFICADO")),
            cod_status_num,
            str(row.get("auditoria_fase", "NAO_IDENTIFICADO")),
            bool(row.get("auditoria_possui_espelho", True)),
            p_cal_1,
            set_conf_lista,
            len(set_conf_lista),
            False,                      # estagio2_executado
            None,                       # n2_classe_predita
            None,                       # n2_cot_justificativa
            None,                       # n2_delta_cot_dispersao
            None,                       # n2_log_odds_atualizado
            p_final,
            rota,
            faixa,
            float(row.get("custo_estimado_usd", 0.0)) if pd.notna(row.get("custo_estimado_usd")) else 0.0,
            float(row.get("tempo_segundos", 0.0)) if pd.notna(row.get("tempo_segundos")) else 0.0,
            datetime.utcnow(),
        ))

        # 3. Tabela: fila_trabalho (Goldenset nasce como já concluído pela IA)
        registros_fila.append((
            fila_uuid,
            proc_uuid,
            None,                                           # usuario_atribuido_id
            "JARISTA",                                      # perfil_requerido
            StatusProcessamento.CONCLUIDO_IA.value,         # status
            float(p_final),                                 # prioridade_ordenacao
            None,                                           # id_bloco_sei
            None,                                           # data_distribuicao
            datetime.utcnow(),                              # atualizado_em
        ))

    return registros_processos, registros_predicoes, registros_fila


def executar_carga_banco(
    db_url: str,
    processos: List[Tuple],
    predicoes: List[Tuple],
    fila: List[Tuple],
) -> None:
    """Executa a carga em lote transacional (ACID) com upsert no Cloud SQL."""
    logger.info(f"Conectando ao banco de dados: {db_url.split('@')[-1] if '@' in db_url else db_url}")

    conn = psycopg2.connect(db_url)
    try:
        with conn.cursor() as cursor:
            # 1. Inserção na tabela processos_administrativos (com ON CONFLICT DO NOTHING)
            query_proc = """
                INSERT INTO processos_administrativos (
                    processo_id, numero_processo_sei, id_procedimento_sei,
                    numero_auto_infracao, placa_veiculo, cpf_cnpj_requerente,
                    data_autuacao, origem_lote, nome_arquivo_origem,
                    hash_sha256_documento, criado_em
                ) VALUES %s
                ON CONFLICT (numero_processo_sei) DO UPDATE SET
                    nome_arquivo_origem = EXCLUDED.nome_arquivo_origem,
                    origem_lote = EXCLUDED.origem_lote;
            """
            logger.info(f"Gravando {len(processos)} registros em processos_administrativos...")
            execute_values(cursor, query_proc, processos)

            # 2. Inserção na tabela predicoes_pipeline
            query_pred = """
                INSERT INTO predicoes_pipeline (
                    predicao_id, processo_id, n1_classe_predita, n1_encaminhamento,
                    n1_cot_justificativa, n1_lauda_evidencia, n1_frase_citada,
                    n1_tipo_citacao, feat_fuzz_ratio, feat_token_containment,
                    feat_exact_match, feat_adversarial_density, feat_sim_centroid_pred,
                    feat_mahalanobis_dist, feat_is_manuscrito, feat_tesseract_mean_conf,
                    auditoria_prefixo_auto, auditoria_codigo_status,
                    auditoria_fase_descricao, auditoria_possui_espelho,
                    probabilidade_calibrada_n1, conjunto_conformal,
                    tamanho_conjunto_conformal, estagio2_executado,
                    n2_classe_predita, n2_cot_justificativa, n2_delta_cot_dispersao,
                    n2_log_odds_atualizado, probabilidade_final, rota_execucao,
                    faixa_operacional, custo_estimado_usd, tempo_execucao_s,
                    executado_em
                ) VALUES %s
                ON CONFLICT (predicao_id) DO NOTHING;
            """
            logger.info(f"Gravando {len(predicoes)} registros em predicoes_pipeline...")
            execute_values(cursor, query_pred, predicoes)

            # 3. Inserção na tabela fila_trabalho
            query_fila = """
                INSERT INTO fila_trabalho (
                    fila_id, processo_id, usuario_atribuido_id, perfil_requerido,
                    status, prioridade_ordenacao, id_bloco_sei, data_distribuicao,
                    atualizado_em
                ) VALUES %s
                ON CONFLICT (fila_id) DO NOTHING;
            """
            logger.info(f"Gravando {len(fila)} registros em fila_trabalho...")
            execute_values(cursor, query_fila, fila)

            conn.commit()
            logger.info("✅ Transação efetivada com sucesso no Cloud SQL!")

    except Exception as e:
        conn.rollback()
        logger.error(f"❌ Falha na transação. Rollback executado: {e}")
        raise
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(
        description="Script de Carga (Seed) do Goldenset para o Cloud SQL."
    )
    parser.add_argument(
        "--artifacts-dir",
        default="artifacts/v3-gemini-3.7-flash",
        help="Caminho relativo dos artefatos consolidados da v3.",
    )
    parser.add_argument(
        "--db-url",
        default=DATABASE_URL,
        help="URL de conexão PostgreSQL (Cloud SQL).",
    )
    args = parser.parse_args()

    # Localização dos arquivos da v3 no repositório de experimentos
    dir_artefatos = REPO_ROOT / args.artifacts_dir
    if not dir_artefatos.exists():
        # Fallback para busca direta dentro da pasta da pipeline se copiado para lá
        dir_artefatos = PIPELINE_DIR / args.artifacts_dir

    candidatos_csv = sorted(
        dir_artefatos.glob("base_calibracao_full_*.csv"),
        key=lambda x: x.stat().st_mtime,
        reverse=True,
    )
    candidatos_json = sorted(
        dir_artefatos.glob("decisoes_detalhadas_full_*.json"),
        key=lambda x: x.stat().st_mtime,
        reverse=True,
    )

    if not candidatos_csv or not candidatos_json:
        raise FileNotFoundError(
            f"Artefatos full não localizados em {dir_artefatos}. "
            "Certifique-se de que o CSV e JSON da v3 estão disponíveis."
        )

    csv_path = candidatos_csv[0]
    json_path = candidatos_json[0]

    logger.info("=" * 80)
    logger.info("INICIANDO MIGRAÇÃO DO GOLDENSET PARA O CLOUD SQL")
    logger.info(f"Origem CSV:  {csv_path}")
    logger.info(f"Origem JSON: {json_path}")
    logger.info("=" * 80)

    df_cal, dict_json = carregar_arquivos_origem(csv_path, json_path)
    processos, predicoes, fila = processar_registros_para_banco(df_cal, dict_json)

    executar_carga_banco(
        db_url=args.db_url,
        processos=processos,
        predicoes=predicoes,
        fila=fila,
    )

    logger.info("=" * 80)
    logger.info(f"Seed concluído: {len(processos)} processos ingeridos no banco.")
    logger.info("=" * 80)


if __name__ == "__main__":
    main()