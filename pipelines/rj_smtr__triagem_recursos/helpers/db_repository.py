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
   a telemetria de tokens/custos e a transição de status da fila.

Fluxo de Dependências:
- Consumidores Diretos:
  * `tasks.py`: invoca `buscar_processos_para_triagem_ou_fallback` na busca de processos
    e `persistir_lote_resultados_ia` ao término da triagem concorrente.
  * `utils/artifacts.py`: consome `buscar_configuracao_calibracao_ativa` para instanciar
    o calibrador a partir dos parâmetros persistidos em banco.
- Dependências Diretas:
  * `constants.py`: importa os nomes canônicos de tabelas (`TABELA_PROCESSOS`, `TABELA_FILA`,
    `TABELA_PREDICOES`, `TABELA_CALIBRACAO`) e os valores de `StatusProcessamento`.
  * `queries/*.sql`: templates SQL carregados via `load_query`.
  * `prefect.logging`: logger pré-configurado do workspace.
"""

import json
import re
import string
from pathlib import Path
from typing import Any

import numpy as np
import psycopg2
from psycopg2.extras import RealDictCursor, execute_values
from prefect_rj_iplanrio.logging import get_logger

logger = get_logger(__name__)

from pipelines.rj_smtr__triagem_recursos.constants import (
    LOCAL_PDFS_DIR,
    PIPELINE_DIR,
    TABELA_CALIBRACAO,
    TABELA_FILA,
    TABELA_PREDICOES,
    TABELA_PROCESSOS,
    StatusProcessamento,
)


def load_query(anchor: str, name: str, **kwargs: Any) -> str:
    """Load and render an external SQL template from `queries/`.

    :param anchor: Reference module file path.
    :param name: SQL filename without `.sql` extension.
    :returns: Rendered SQL query string.
    """
    sql_path = Path(anchor).parent / "queries" / f"{name}.sql"
    if not sql_path.exists():
        sql_path = PIPELINE_DIR / "queries" / f"{name}.sql"
    with open(sql_path, encoding="utf-8") as sql_file:
        raw_sql = sql_file.read()
    raw_sql = re.sub(r"\{#.*?#\}", "", raw_sql, flags=re.DOTALL)
    template = string.Template(raw_sql)
    return template.safe_substitute(**kwargs)


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


def recuperar_processos_travados(cursor: Any, timeout_minutos: int = 30) -> int:
    """Recupera itens que ficaram presos em PROCESSANDO_IA alem do tempo limite."""
    query = load_query(
        str(PIPELINE_DIR / "flow.py"),
        "recuperar_processos_travados",
        tabela_fila=TABELA_FILA,
    )
    cursor.execute(query, (timeout_minutos,))
    return int(cursor.rowcount)


def buscar_processos_para_triagem(
    db_url: str,
    modo: str = "INCREMENTAL",
    limite: int = 300,
    data_inicio: str | None = None,
    data_fim: str | None = None,
) -> list[dict[str, Any]]:
    """Busca e reserva atomicamente processos na fila de trabalho com status PENDENTE_IA."""
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
            # 1. Recupera processos presos em PROCESSANDO_IA por mais de 30 minutos
            recuperados = recuperar_processos_travados(cursor, timeout_minutos=30)
            if recuperados > 0:
                logger.warning("Fila: %d processo(s) travado(s) recuperado(s) para PENDENTE_IA.", recuperados)

            # 2. Reserva atomica com SKIP LOCKED e atualizacao imediata para PROCESSANDO_IA
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
            processos = [dict(r) for r in cursor.fetchall()]
        conn.commit()
        return processos
    except Exception:
        conn.rollback()
        raise
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
    query_update_doc = load_query(
        str(PIPELINE_DIR / "flow.py"),
        "update_processo_documento",
        tabela_processos=TABELA_PROCESSOS,
    )

    conn = psycopg2.connect(db_url)
    try:
        with conn.cursor() as cursor:
            execute_values(cursor, query_predicoes, valores)

            for r in resultados:
                doc_raw = str(r.get("id_documento_sei") or "").strip()
                if doc_raw and r.get("processo_id"):
                    primeiro_doc = doc_raw.split(",")[0].strip()
                    if primeiro_doc:
                        cursor.execute(query_update_doc, (primeiro_doc, str(r["processo_id"])))

            ids_sucesso = [
                r["processo_id"]
                for r in resultados
                if r.get("status_fila") != StatusProcessamento.ERRO_PROCESSAMENTO.value
            ]
            ids_erro = [
                r["processo_id"]
                for r in resultados
                if r.get("status_fila") == StatusProcessamento.ERRO_PROCESSAMENTO.value
            ]

            if ids_sucesso:
                cursor.execute(
                    query_update_fila,
                    (StatusProcessamento.CONCLUIDO_IA.value, ids_sucesso),
                )
            if ids_erro:
                cursor.execute(
                    query_update_fila,
                    (StatusProcessamento.ERRO_PROCESSAMENTO.value, ids_erro),
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


def registrar_processo_e_enfileirar(
    cursor: Any,
    query_upsert_proc: str,
    query_enfileirar: str,
    numero_sei: str,
    id_procedimento_sei: str | None,
    id_documento_sei: str | None,
    numero_auto_infracao: str,
    origem_lote: str,
) -> str:
    """Insere ou atualiza processo em `processos_administrativos` e garante entrada na fila."""
    doc_principal = id_documento_sei.split(",")[0].strip() if id_documento_sei else None
    cursor.execute(
        query_upsert_proc,
        (
            numero_sei[:32],
            str(id_procedimento_sei)[:32] if id_procedimento_sei else None,
            str(doc_principal)[:64] if doc_principal else None,
            str(numero_auto_infracao or "AUTO_SEI")[:20],
            origem_lote,
        ),
    )
    row = cursor.fetchone()
    if not row:
        raise RuntimeError(f"Upsert de {numero_sei} não retornou processo_id.")
    proc_id = str(row[0])
    cursor.execute(
        query_enfileirar,
        (proc_id, StatusProcessamento.PENDENTE_IA.value, proc_id),
    )
    return proc_id


def buscar_processos_para_triagem_ou_fallback(
    db_url: str,
    is_local: bool = False,
    fonte_pdf: str = "sei",
    cliente_sei: Any | None = None,
    modo: str = "INCREMENTAL",
    limite: int = 300,
    data_inicio: str | None = None,
    data_fim: str | None = None,
    arquivos_especificos: list[Any] | None = None,
) -> list[dict[str, Any]]:
    """Consulta ou filtra os processos elegíveis para triagem (fail-fast em cloud/staging).

    :param db_url: URL de conexão do PostgreSQL.
    :param is_local: Permite fallback em memória apenas quando True e fonte_pdf == 'local'.
    :param fonte_pdf: "local" para PDFs em disco ou "sei" para integração direta.
    :param cliente_sei: Cliente do SEI com listar_processos_caixa().
    :param modo: Modo de processamento (INCREMENTAL ou PASSIVO).
    :param limite: Quantidade máxima de processos a processar.
    :param data_inicio: Filtro temporal inicial opcional.
    :param data_fim: Filtro temporal final opcional.
    :param arquivos_especificos: Lista opcional pré-selecionada de arquivos ou dicionários.
    :returns: Lista de processos estruturados para a triagem.
    """
    origem_lote_val = "PASSIVO" if modo.upper() == "PASSIVO" else "MENSAL"

    if arquivos_especificos:
        logger.info(
            "Processando lista específica de %d arquivos fornecidos.",
            len(arquivos_especificos),
        )
        lista_proc: list[dict[str, Any]] = []
        conn = None
        cursor = None
        query_upsert_proc = ""
        query_enfileirar = ""
        try:
            conn = psycopg2.connect(db_url)
            cursor = conn.cursor()
            query_upsert_proc = load_query(
                str(PIPELINE_DIR / "flow.py"),
                "upsert_processo_sei",
                tabela_processos=TABELA_PROCESSOS,
            )
            query_enfileirar = load_query(
                str(PIPELINE_DIR / "flow.py"),
                "enfileirar_processo",
                tabela_fila=TABELA_FILA,
            )
        except Exception as exc:
            if not is_local or fonte_pdf != "local":
                raise RuntimeError(f"Banco inacessível para registrar arquivos_especificos: {exc}") from exc
            logger.warning("Banco local inacessível (%s). Usando IDs em memória.", exc)
            conn = None
            cursor = None

        try:
            for i, item in enumerate(arquivos_especificos[:limite], 1):
                if isinstance(item, dict):
                    nome = item.get("nome_arquivo_origem") or item.get("arquivo") or ""
                    num_sei = str(
                        item.get("numero_processo_sei") or (f"SEI-{Path(nome).stem[:28]}" if nome else f"SEI-{i}")
                    )
                    id_proc = item.get("id_procedimento_sei") or item.get("id_procedimento")
                    id_doc = item.get("id_documento_sei") or item.get("id_documento")
                    auto = str(item.get("numero_auto_infracao") or item.get("descricao_auto") or "AUTO_SEI")[:20]
                    p_id = item.get("processo_id")
                    extra = {
                        "data_autuacao": item.get("data_autuacao"),
                        "ouro_hipotese": item.get("ouro_hipotese"),
                        "ouro_encaminhamento": item.get("ouro_encaminhamento"),
                    }
                else:
                    nome = item if isinstance(item, str) else item.name
                    num_sei = f"SEI-{Path(nome).stem[:28]}"
                    id_proc = None
                    id_doc = None
                    auto = "AUTO_LOCAL"
                    p_id = None
                    extra = {"data_autuacao": None, "ouro_hipotese": None, "ouro_encaminhamento": None}

                if not p_id and cursor:
                    p_id = registrar_processo_e_enfileirar(
                        cursor=cursor,
                        query_upsert_proc=query_upsert_proc,
                        query_enfileirar=query_enfileirar,
                        numero_sei=num_sei,
                        id_procedimento_sei=str(id_proc) if id_proc else None,
                        id_documento_sei=str(id_doc) if id_doc else None,
                        numero_auto_infracao=auto,
                        origem_lote=origem_lote_val,
                    )

                if not p_id:
                    if not is_local or fonte_pdf != "local":
                        raise RuntimeError(f"Falha ao obter processo_id real no banco para {num_sei}.")
                    p_id = f"00000000-0000-0000-0000-{i:012d}"

                lista_proc.append(
                    {
                        "processo_id": str(p_id),
                        "numero_processo_sei": num_sei,
                        "id_documento_sei": str(id_doc) if id_doc else None,
                        "id_procedimento_sei": str(id_proc) if id_proc else None,
                        "numero_auto_infracao": auto,
                        "nome_arquivo_origem": nome,
                        **extra,
                    }
                )
            if conn:
                conn.commit()
        except Exception:
            if conn:
                conn.rollback()
            raise
        finally:
            if conn:
                conn.close()
        return lista_proc

    if fonte_pdf == "sei":
        if cliente_sei is None:
            raise RuntimeError("cliente_sei obrigatório quando fonte_pdf='sei'.")
        logger.info("Listando processos diretamente da caixa da JARI no SEI...")
        processos_caixa = cliente_sei.listar_processos_caixa()
        if not processos_caixa:
            logger.info("Caixa da JARI no SEI vazia.")
            return []

        numeros_sei = [str(p.get("numero_processo_sei") or p.get("numero_processo")) for p in processos_caixa]
        ja_triados = set(filtrar_processos_ja_triados(db_url, numeros_sei))

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

        processos_sincronizados: list[dict[str, Any]] = []
        conn = psycopg2.connect(db_url)
        try:
            query_upsert_proc = load_query(
                str(PIPELINE_DIR / "flow.py"),
                "upsert_processo_sei",
                tabela_processos=TABELA_PROCESSOS,
            )
            query_enfileirar = load_query(
                str(PIPELINE_DIR / "flow.py"),
                "enfileirar_processo",
                tabela_fila=TABELA_FILA,
            )

            with conn.cursor() as cursor:
                for p in processos_pendentes[:limite]:
                    num_sei = str(p.get("numero_processo_sei") or p.get("numero_processo"))
                    id_proc = str(p.get("id_procedimento_sei") or "")
                    id_doc = str(p.get("id_documento_sei") or "")

                    proc_id = registrar_processo_e_enfileirar(
                        cursor=cursor,
                        query_upsert_proc=query_upsert_proc,
                        query_enfileirar=query_enfileirar,
                        numero_sei=num_sei,
                        id_procedimento_sei=id_proc or None,
                        id_documento_sei=id_doc or None,
                        numero_auto_infracao="AUTO_SEI",
                        origem_lote=origem_lote_val,
                    )

                    p_item = dict(p)
                    p_item["processo_id"] = proc_id
                    p_item["id_documento_sei"] = id_doc or None
                    p_item["numero_auto_infracao"] = "AUTO_SEI"
                    p_item["nome_arquivo_origem"] = f"{num_sei.replace('/', '_')}.pdf"
                    processos_sincronizados.append(p_item)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
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
        if not is_local or fonte_pdf != "local":
            logger.error("Falha crítica ao consultar fila no banco: %s", exc)
            raise
        logger.warning(
            "Falha ao consultar fila local (%s). Verificando PDFs em %s...",
            exc,
            LOCAL_PDFS_DIR,
        )

        def chave_ordenacao(p: Path) -> int:
            try:
                return int(p.stem.split("(")[1].split(")")[0])
            except Exception:
                return 0

        arquivos_req = sorted(
            LOCAL_PDFS_DIR.glob("REQUERIMENTOS INTEMPESTIVOS*.pdf"),
            key=chave_ordenacao,
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
