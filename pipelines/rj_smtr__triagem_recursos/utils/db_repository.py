"""Repositório transacional e camada de persistência para o banco PostgreSQL / Cloud SQL da SMTR.

Descrição Técnica & Arquitetura:
Centraliza todas as operações de banco de dados do sistema de triagem. Implementa
o isolamento transacional, a integridade referencial e o controle de idempotência
para evitar duplicidade no reprocessamento de lotes. Responsável por:
1. Buscar e bloquear processos na fila com status PENDENTE_IA.
2. Sincronizar processos recém-ingeridos da caixa JARI no SEI-Rio com a tabela
   `processos_administrativos` e criar as respectivas entradas em `fila_trabalho`.
3. Filtrar processos já triados (status CONCLUIDO_IA) garantindo execução idempotente.
4. Recuperar a calibração tabular ativa (LightGBM e quantis conformais CCCP).
5. Persistir em lote (batch insert) as predições de IA, os 8 sinais epistêmicos,
   a telemetria de tokens/custos, a transição de status da fila e o registro
   do evento auditável TRIADO_IA na tabela `eventos`.

Fluxo de Dependências:
- Consumidores Diretos:
  * `tasks.py`: invoca `buscar_processos_para_triagem_ou_fallback` na busca de processos
    e `persistir_lote_resultados_ia` ao término da triagem concorrente.
  * `utils/artifacts.py`: consome `buscar_configuracao_calibracao_ativa` para instanciar
    o calibrador a partir dos parâmetros persistidos em banco.
  * `scripts/setup_local_db.py`: executa a inicialização do esquema DDL consolidado.
- Dependências Diretas:
  * `constants.py`: importa os nomes canônicos de tabelas (`TABELA_PROCESSOS`, `TABELA_FILA`,
    `TABELA_PREDICOES`, etc.) e os valores de `StatusProcessamento`.
  * `prefect_rj_iplanrio.sql`: utiliza a função `load_query` para carregar e compilar
    os arquivos SQL externos de `queries/*.sql`.
  * `prefect_rj_iplanrio.logging`: logger pré-configurado do workspace.
"""

import json
from pathlib import Path
from typing import Any

import numpy as np
import psycopg2
from psycopg2.extras import RealDictCursor, execute_values

from prefect_rj_iplanrio.logging import get_logger
from prefect_rj_iplanrio.sql import load_query

from ..constants import (
    LOCAL_PDFS_DIR,
    PIPELINE_DIR,
    TABELA_CALIBRACAO,
    TABELA_FILA,
    TABELA_PREDICOES,
    TABELA_PROCESSOS,
    StatusProcessamento,
)

logger = get_logger(__name__)


def converter_valor_para_sql_null(valor: Any) -> float | None:
    """Converte np.nan ou None para None (SQL NULL), senão float.

    :param valor: Valor de entrada numérico ou nulo.
    :returns: Float válido ou None.
    """
    if valor is None:
        return None
    try:
        val = float(valor)
        return None if np.isnan(val) else val
    except (ValueError, TypeError):
        return None


def buscar_configuracao_calibracao_ativa(db_url: str) -> dict[str, Any]:
    """Recupera a configuração ativa de calibração persistida no PostgreSQL.

    :param db_url: URL de conexão do PostgreSQL (local ou Cloud SQL).
    :returns: Dicionário com parâmetros puros, quantis e modelo textual do LightGBM.
    :raises RuntimeError: Caso nenhuma configuração ativa seja encontrada.
    """
    query = load_query(
        str(PIPELINE_DIR / "flow.py"),
        "buscar_configuracao_calibracao_ativa",
        tabela_calibracao=TABELA_CALIBRACAO,
    )
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


def filtrar_processos_ja_triados(db_url: str, numeros_processos_sei: list[str]) -> list[str]:
    """Retorna a lista de números SEI que já foram triados (status CONCLUIDO_IA).

    :param db_url: URL de conexão do PostgreSQL.
    :param numeros_processos_sei: Lista de identificadores SEI para conferência.
    :returns: Lista de identificadores que já constam concluídos no banco.
    """
    if not numeros_processos_sei:
        return []

    query = load_query(
        str(PIPELINE_DIR / "flow.py"),
        "filtrar_processos_ja_triados",
        tabela_processos=TABELA_PROCESSOS,
        tabela_fila=TABELA_FILA,
    )
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
    data_inicio: str | None = None,
    data_fim: str | None = None,
) -> list[dict[str, Any]]:
    """Busca processos na fila de trabalho com status PENDENTE_IA.

    :param db_url: URL de conexão do PostgreSQL.
    :param modo: Modo operacional (INCREMENTAL para MENSAL ou PASSIVO).
    :param limite: Quantidade máxima de registros a recuperar.
    :param data_inicio: Filtro temporal inicial opcional.
    :param data_fim: Filtro temporal final opcional.
    :returns: Lista de dicionários representando os processos pendentes.
    """
    origem_alvo = "MENSAL" if modo.upper() == "INCREMENTAL" else "PASSIVO"

    query = load_query(
        str(PIPELINE_DIR / "flow.py"),
        "buscar_processos_para_triagem",
        tabela_processos=TABELA_PROCESSOS,
        tabela_fila=TABELA_FILA,
    )

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


def persistir_lote_resultados_ia(db_url: str, resultados: list[dict[str, Any]]) -> None:
    """Persiste as predições, sinais epistêmicos e auditoria de sistema no PostgreSQL.

    :param db_url: URL de conexão do PostgreSQL.
    :param resultados: Lista de predições consolidadas pelo pipeline.
    :raises Exception: Propaga exceções de persistência com rollback atômico.
    """
    if not resultados:
        return

    query_predicoes = load_query(
        str(PIPELINE_DIR / "flow.py"),
        "insert_predicoes",
        tabela_predicoes=TABELA_PREDICOES,
    )

    valores = []
    processos_ids = []

    for r in resultados:
        processos_ids.append(r["processo_id"])
        sinais = r.get("sinais_epistemicos", {})
        auditoria = r.get("auditoria_sistema", {})

        valores.append(
            (
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
                converter_valor_para_sql_null(sinais.get("mahalanobis_dist")),
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
                converter_valor_para_sql_null(r.get("n2_delta_cot_dispersao")),
                converter_valor_para_sql_null(r.get("n2_log_odds_atualizado")),
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
            )
        )

    query_update_fila = load_query(
        str(PIPELINE_DIR / "flow.py"),
        "update_status_fila",
        tabela_fila=TABELA_FILA,
    )

    conn = psycopg2.connect(db_url)
    try:
        with conn.cursor() as cursor:
            execute_values(cursor, query_predicoes, valores)
            cursor.execute(
                query_update_fila,
                (StatusProcessamento.CONCLUIDO_IA.value, processos_ids),
            )
            for r in resultados:
                cursor.execute(
                    """
                    INSERT INTO eventos (processo_id, usuario_sei, tipo_evento, detalhes_json)
                    VALUES (%s, %s, %s, %s);
                    """,
                    (
                        r["processo_id"],
                        "prefect.pipeline",
                        "TRIADO_IA",
                        json.dumps(
                            {
                                "classe_predita": r["n1_classe_predita"],
                                "faixa": r["faixa_operacional"],
                                "probabilidade": float(r["probabilidade_final"]),
                                "rota": r["rota_execucao"],
                            }
                        ),
                    ),
                )
        conn.commit()
        logger.info(
            "Gravados %d resultados no banco de triagem com sucesso.",
            len(resultados),
        )
    except Exception as exc:
        conn.rollback()
        logger.error("Erro ao persistir lote no banco de dados: %s", exc)
        raise
    finally:
        conn.close()


def buscar_processos_para_triagem_ou_fallback(
    db_url: str,
    fonte_pdf: str = "local",
    cliente_sei: Any | None = None,
    modo: str = "INCREMENTAL",
    limite: int = 300,
    data_inicio: str | None = None,
    data_fim: str | None = None,
    arquivos_especificos: list[Any] | None = None,
) -> list[dict[str, Any]]:
    """Consulta ou filtra os processos elegíveis para triagem com suporte a fallbacks.

    :param db_url: URL de conexão do PostgreSQL.
    :param fonte_pdf: "local" para PDFs em disco ou "sei" para integração direta.
    :param cliente_sei: Cliente do SEI com listar_processos_caixa().
    :param modo: Modo de processamento (INCREMENTAL ou PASSIVO).
    :param limite: Quantidade máxima de processos a processar.
    :param data_inicio: Filtro temporal inicial opcional.
    :param data_fim: Filtro temporal final opcional.
    :param arquivos_especificos: Lista opcional pré-selecionada de arquivos ou dicionários.
    :returns: Lista de processos estruturados para a triagem.
    """
    if arquivos_especificos:
        logger.info(
            "Processando lista específica de %d arquivos fornecidos.",
            len(arquivos_especificos),
        )
        lista_proc = []
        for i, item in enumerate(arquivos_especificos[:limite], 1):
            if isinstance(item, dict):
                nome = item.get("nome_arquivo_origem") or item.get("arquivo")
                lista_proc.append(
                    {
                        "processo_id": item.get("processo_id") or f"00000000-0000-0000-0000-{i:012d}",
                        "numero_processo_sei": item.get("numero_processo_sei") or f"SEI-{Path(nome).stem[:28]}",
                        "id_documento_sei": item.get("id_documento_sei"),
                        "id_procedimento_sei": item.get("id_procedimento_sei"),
                        "numero_auto_infracao": item.get("numero_auto_infracao") or "AUTO_LOCAL",
                        "nome_arquivo_origem": nome,
                        "data_autuacao": item.get("data_autuacao"),
                        "ouro_hipotese": item.get("ouro_hipotese"),
                        "ouro_encaminhamento": item.get("ouro_encaminhamento"),
                    }
                )
            else:
                nome = item if isinstance(item, str) else item.name
                lista_proc.append(
                    {
                        "processo_id": f"00000000-0000-0000-0000-{i:012d}",
                        "numero_processo_sei": f"SEI-{Path(nome).stem[:28]}",
                        "id_documento_sei": None,
                        "id_procedimento_sei": None,
                        "numero_auto_infracao": "AUTO_LOCAL",
                        "nome_arquivo_origem": nome,
                        "data_autuacao": None,
                        "ouro_hipotese": None,
                        "ouro_encaminhamento": None,
                    }
                )
        return lista_proc

    if fonte_pdf == "sei" and cliente_sei is not None:
        logger.info("Listando processos diretamente da caixa da JARI no SEI...")
        processos_caixa = cliente_sei.listar_processos_caixa()
        if not processos_caixa:
            logger.info("Caixa da JARI no SEI vazia.")
            return []

        numeros_sei = [str(p.get("numero_processo_sei") or p.get("numero_processo")) for p in processos_caixa]

        try:
            ja_triados = set(filtrar_processos_ja_triados(db_url, numeros_sei))
        except Exception as exc:
            logger.warning(
                "Falha ao consultar banco para idempotência (%s). Processando lote da caixa.",
                exc,
            )
            ja_triados = set()

        processos_pendentes = [
            p
            for p in processos_caixa
            if str(p.get("numero_processo_sei") or p.get("numero_processo")) not in ja_triados
        ]
        logger.info(
            "SEI: %d processo(s) na caixa | %d já triado(s) | %d a triar.",
            len(processos_caixa),
            len(ja_triados),
            len(processos_pendentes),
        )

        processos_sincronizados = []
        conn = psycopg2.connect(db_url)
        try:
            with conn.cursor() as cursor:
                for p in processos_pendentes[:limite]:
                    num_sei = str(p.get("numero_processo_sei") or p.get("numero_processo"))
                    id_proc = str(p.get("id_procedimento_sei") or "")
                    id_doc = str(p.get("id_documento_sei") or "")
                    codigo_jari = str(p.get("sigla_unidade") or "1_JARI")
                    relator = str(p.get("usuario_atribuido_sigla") or "")
                    status_dist = "DISTRIBUIDO" if relator else "PENDENTE"

                    cursor.execute(
                        """
                        INSERT INTO processos_administrativos (
                            numero_processo_sei,
                            id_procedimento_sei,
                            id_documento_sei,
                            numero_auto_infracao,
                            origem_lote,
                            codigo_jari,
                            atribuido_a_login_sei,
                            status_distribuicao
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (numero_processo_sei) DO UPDATE SET
                            id_procedimento_sei = COALESCE(EXCLUDED.id_procedimento_sei, processos_administrativos.id_procedimento_sei),
                            id_documento_sei = COALESCE(EXCLUDED.id_documento_sei, processos_administrativos.id_documento_sei),
                            atribuido_a_login_sei = COALESCE(EXCLUDED.atribuido_a_login_sei, processos_administrativos.atribuido_a_login_sei),
                            status_distribuicao = EXCLUDED.status_distribuicao
                        RETURNING processo_id;
                        """,
                        (
                            num_sei,
                            id_proc or None,
                            id_doc or None,
                            "AUTO_SEI",
                            "MENSAL",
                            codigo_jari,
                            relator or None,
                            status_dist,
                        ),
                    )
                    proc_id = cursor.fetchone()[0]

                    cursor.execute(
                        """
                        INSERT INTO fila_trabalho (processo_id, status)
                        VALUES (%s, %s)
                        ON CONFLICT DO NOTHING;
                        """,
                        (proc_id, StatusProcessamento.PENDENTE_IA.value),
                    )

                    p_item = dict(p)
                    p_item["processo_id"] = str(proc_id)
                    p_item["numero_auto_infracao"] = "AUTO_SEI"
                    p_item["nome_arquivo_origem"] = f"{num_sei.replace('/', '_')}.pdf"
                    processos_sincronizados.append(p_item)
            conn.commit()
        finally:
            conn.close()

        return processos_sincronizados

    logger.info(
        "Buscando processos na fila do banco | Modo: %s | Limite: %d",
        modo,
        limite,
    )
    try:
        processos = buscar_processos_para_triagem(
            db_url=db_url,
            modo=modo,
            limite=limite,
            data_inicio=data_inicio,
            data_fim=data_fim,
        )
        logger.info("%d processo(s) elegível(is) encontrado(s) no banco.", len(processos))
        return processos
    except Exception as exc:
        logger.warning(
            "Falha ao consultar fila no banco (%s). Verificando PDFs na pasta local...",
            exc,
        )

        def _chave_ordenacao(p: Path) -> int:
            try:
                return int(p.stem.split("(")[1].split(")")[0])
            except Exception:
                return 0

        arquivos_req = sorted(
            LOCAL_PDFS_DIR.glob("REQUERIMENTOS INTEMPESTIVOS*.pdf"),
            key=_chave_ordenacao,
        )
        arquivos_locais = arquivos_req if arquivos_req else sorted(LOCAL_PDFS_DIR.glob("*.pdf"))
        return [
            {
                "processo_id": f"00000000-0000-0000-0000-{i:012d}",
                "numero_processo_sei": f"SEI-{arq.stem[:28]}",
                "id_documento_sei": None,
                "id_procedimento_sei": None,
                "numero_auto_infracao": "AUTO_LOCAL",
                "nome_arquivo_origem": arq.name,
                "data_autuacao": None,
            }
            for i, arq in enumerate(arquivos_locais[:limite], 1)
        ]
