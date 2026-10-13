# -*- coding: utf-8 -*-
"""Banco de exemplos revisados (RAG) — ingestão das revisões da secretaria e leitura
pro few-shot da classificação.

Ingestão (ingere_revisoes), no início de toda solicitação "Relatório de engajamento",
só da campanha da solicitação:
  1. acha os Sheets com MARCADOR_REVISADA no nome em qualquer subpasta da pasta da
     campanha — ignorando pastas de relatório rápido (SUFIXO_PASTA_RAPIDO);
  2. sincroniza com a tabela RAG_TABLE: arquivo novo/alterado (modifiedTime) ou
     embedado com outro modelo é reprocessado; arquivo que sumiu (apagado, renomeado
     sem 'revisada') tem os exemplos removidos; arquivo inalterado não é nem lido;
  3. cada linha revisada vira exemplo com as categorias CORRETAS (Certo -> categoria;
     Errado -> categorias_corretas), validadas contra o Catálogo da campanha.

Isolamento por campanha: o exemplo é gravado com a campanha da PASTA onde o Sheet
está, as categorias precisam existir no Catálogo dessa campanha e carrega_exemplos
filtra por campanha na query — exemplo de uma campanha nunca entra no few-shot de outra.

Escrita por load job (não streaming): linha inserida por streaming fica horas sem
poder ser apagada por DML, o que quebraria a substituição de um arquivo alterado."""

from __future__ import annotations

import re
from collections import Counter
from dataclasses import dataclass
from datetime import date, datetime, timezone

import numpy as np
import pandas as pd
from google.cloud import bigquery
from googleapiclient.discovery import Resource
from iplanrio.pipelines_utils.logging import log
from prefect import task

from pipelines.rj_crm__relatorio_engajamento_hsm.config import (
    COL_CATEGORIAS_CORRETAS,
    COL_CONV_CATEGORIA,
    COL_CONV_CONVERSA,
    COL_CONV_ID_SESSAO,
    COL_JUSTIFICATIVA_REVISAO,
    COL_REVISAO,
    DRIVE_SHEET_MIME,
    EMBEDDING_MODEL,
    MARCADOR_REVISADA,
    RAG_TABLE,
    REVISAO_CERTO,
    REVISAO_ERRADO,
    SEP_CATEGORIAS,
    SUFIXO_PASTA_RAPIDO,
)
from pipelines.rj_crm__relatorio_engajamento_hsm.utils.bigquery import get_bq_client
from pipelines.rj_crm__relatorio_engajamento_hsm.utils.catalogo import CatalogoValidado
from pipelines.rj_crm__relatorio_engajamento_hsm.utils.drive import get_drive_service, lista_recursivo
from pipelines.rj_crm__relatorio_engajamento_hsm.utils.embedding import gera_embeddings_conversas
from pipelines.rj_crm__relatorio_engajamento_hsm.utils.sheets import COL_LINHA, get_sheets_service, le_sheet

SCHEMA_RAG = [
    bigquery.SchemaField("nome_campanha", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("id_sessao", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("conversa", "STRING"),
    bigquery.SchemaField("categorias", "STRING", mode="REPEATED"),
    bigquery.SchemaField("revisao", "STRING"),
    bigquery.SchemaField("justificativa_revisao", "STRING"),
    bigquery.SchemaField("arquivo_origem_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("arquivo_origem_nome", "STRING"),
    # modifiedTime do Drive (RFC 3339) guardado como texto: só é comparado por igualdade.
    bigquery.SchemaField("arquivo_modificado_em", "STRING"),
    bigquery.SchemaField("data_revisao", "DATE"),
    bigquery.SchemaField("embedding", "FLOAT64", mode="REPEATED"),
    bigquery.SchemaField("modelo_embedding", "STRING"),
    bigquery.SchemaField("ingerido_em", "TIMESTAMP"),
]
_COLUNAS_RAG = [campo.name for campo in SCHEMA_RAG]
_REVISOES_POR_CHAVE = {REVISAO_CERTO.casefold(): REVISAO_CERTO, REVISAO_ERRADO.casefold(): REVISAO_ERRADO}
_RE_DATA_REVISAO = re.compile(rf"{MARCADOR_REVISADA}_(\d{{4}}-\d{{2}}-\d{{2}})")


# ============================================================================
# Funções puras — regras de interpretação e de sincronização
# ============================================================================


def data_revisao(nome_arquivo: str, modificado_em: str) -> date | None:
    """Data da revisão: a do sufixo '_revisada_AAAA-MM-DD' do nome; sem ela (ou
    inválida), a data de modificação do arquivo no Drive."""
    achado = _RE_DATA_REVISAO.search(nome_arquivo or "")
    if achado:
        try:
            return date.fromisoformat(achado.group(1))
        except ValueError:
            pass
    try:
        return datetime.fromisoformat(modificado_em.replace("Z", "+00:00")).date()
    except (AttributeError, ValueError):
        return None


def exemplos_da_planilha(df: pd.DataFrame, catalogo: CatalogoValidado) -> tuple[list[dict], list[str]]:
    """Linhas revisadas de UMA planilha conversas_classificadas -> exemplos (dicts com
    id_sessao, conversa, categorias, revisao, justificativa_revisao) + motivos de
    descarte (pro log). Linha sem revisão é só ignorada (ainda não revisada), não é
    descarte. Qualquer categoria fora das ativas do Catálogo descarta a linha inteira."""
    obrigatorias = [COL_CONV_ID_SESSAO, COL_CONV_CATEGORIA, COL_CONV_CONVERSA, COL_REVISAO, COL_CATEGORIAS_CORRETAS]
    faltando = [c for c in obrigatorias if c not in df.columns]
    if faltando:
        return [], [f"planilha sem a(s) coluna(s) {faltando} — ignorada"]

    exemplos, descartes = [], []
    for _, linha in df.iterrows():
        n_linha = linha.get(COL_LINHA, "?")
        revisao_bruta = str(linha[COL_REVISAO]).strip()
        if not revisao_bruta:
            continue
        revisao = _REVISOES_POR_CHAVE.get(revisao_bruta.casefold())
        if revisao is None:
            descartes.append(f"linha {n_linha}: revisão {revisao_bruta!r} (esperado {REVISAO_CERTO}/{REVISAO_ERRADO})")
            continue
        id_sessao = str(linha[COL_CONV_ID_SESSAO]).strip()
        conversa = str(linha[COL_CONV_CONVERSA]).strip()
        if not id_sessao or not conversa:
            descartes.append(f"linha {n_linha}: sem id_sessao ou sem conversa")
            continue

        coluna = COL_CONV_CATEGORIA if revisao == REVISAO_CERTO else COL_CATEGORIAS_CORRETAS
        brutas = [c.strip() for c in str(linha[coluna]).split(SEP_CATEGORIAS) if c.strip()]
        if not brutas:
            descartes.append(f"linha {n_linha}: {revisao} sem categoria em '{coluna}'")
            continue
        canonicas = [catalogo.ativas.busca(c) for c in brutas]
        invalidas = [b for b, c in zip(brutas, canonicas, strict=True) if c is None]
        if invalidas:
            descartes.append(f"linha {n_linha}: categoria(s) {invalidas} não ativa(s) no Catálogo")
            continue

        exemplos.append(
            {
                "id_sessao": id_sessao,
                "conversa": conversa,
                "categorias": list(dict.fromkeys(canonicas)),  # sem repetir, na ordem dada
                "revisao": revisao,
                "justificativa_revisao": str(linha.get(COL_JUSTIFICATIVA_REVISAO, "")).strip(),
            }
        )
    return exemplos, descartes


@dataclass
class PlanoSincronizacao:
    reprocessar: list[dict]  # arquivos do Drive a (re)ler e (re)embedar
    remover: list[str]  # arquivo_origem_id que sumiram do Drive
    inalterados: int


def planeja_sincronizacao(arquivos: list[dict], estado: pd.DataFrame, modelo: str) -> PlanoSincronizacao:
    """arquivos: Sheets revisados achados no Drive ({id, name, modifiedTime, ...}).
    estado: o que a tabela tem da campanha (arquivo_origem_id, arquivo_modificado_em,
    modelo_embedding). Arquivo que gerou 0 exemplos não aparece no estado e é relido a
    cada ingestão — barato (sem embedding) e cobre o caso de alguém revisar depois."""
    gravado = {
        linha.arquivo_origem_id: (linha.arquivo_modificado_em, linha.modelo_embedding) for linha in estado.itertuples()
    }
    reprocessar = [a for a in arquivos if gravado.get(a["id"]) != (a.get("modifiedTime"), modelo)]
    ids_drive = {a["id"] for a in arquivos}
    remover = sorted(i for i in gravado if i not in ids_drive)
    return PlanoSincronizacao(reprocessar, remover, len(arquivos) - len(reprocessar))


def seleciona_exemplos(df: pd.DataFrame, catalogo: CatalogoValidado) -> pd.DataFrame:
    """Exemplos da tabela que valem HOJE: todas as categorias ainda ativas no Catálogo
    atual (categoria excluída depois da revisão tira o exemplo do few-shot, mesmo
    continuando na tabela) e, se a mesma conversa foi revisada em mais de um relatório,
    só a revisão mais recente. Categorias voltam no nome canônico do Catálogo atual."""
    if df.empty:
        return df
    df = df.copy()
    df["categorias"] = [[catalogo.ativas.busca(c) for c in cats] for cats in df["categorias"]]
    df = df[[all(c is not None for c in cats) and len(cats) > 0 for cats in df["categorias"]]]
    df = df.sort_values(["data_revisao", "ingerido_em"], ascending=False, na_position="last")
    return df.drop_duplicates("id_sessao", keep="first").reset_index(drop=True)


# ============================================================================
# BigQuery
# ============================================================================


def _garante_tabela(client: bigquery.Client) -> None:
    client.create_table(bigquery.Table(RAG_TABLE, schema=SCHEMA_RAG), exists_ok=True)


def _estado_campanha(client: bigquery.Client, campanha: str) -> pd.DataFrame:
    sql = f"""
        select distinct arquivo_origem_id, arquivo_modificado_em, modelo_embedding
        from `{RAG_TABLE}`
        where nome_campanha = @campanha
    """
    config = bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter("campanha", "STRING", campanha)])
    return client.query(sql, job_config=config).to_dataframe()


def _remove_arquivos(client: bigquery.Client, campanha: str, ids: list[str]) -> None:
    if not ids:
        return
    sql = f"delete from `{RAG_TABLE}` where nome_campanha = @campanha and arquivo_origem_id in unnest(@ids)"
    config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("campanha", "STRING", campanha),
            bigquery.ArrayQueryParameter("ids", "STRING", ids),
        ]
    )
    client.query(sql, job_config=config).result()


def _insere(client: bigquery.Client, df: pd.DataFrame) -> None:
    if df.empty:
        return
    config = bigquery.LoadJobConfig(schema=SCHEMA_RAG, write_disposition=bigquery.WriteDisposition.WRITE_APPEND)
    client.load_table_from_dataframe(df[_COLUNAS_RAG], RAG_TABLE, job_config=config).result()


# ============================================================================
# Tasks
# ============================================================================


def _linhas_do_arquivo(sheets: Resource, arquivo: dict, campanha: str, catalogo: CatalogoValidado) -> list[dict]:
    exemplos, descartes = exemplos_da_planilha(le_sheet(sheets, arquivo["id"], com_linha=True), catalogo)
    for motivo in descartes:
        log(f"[RAG] {arquivo['name']}: {motivo} — descartada.")
    revisado_em = data_revisao(arquivo["name"], arquivo.get("modifiedTime", ""))
    return [
        {
            **exemplo,
            "nome_campanha": campanha,
            "arquivo_origem_id": arquivo["id"],
            "arquivo_origem_nome": arquivo["name"],
            "arquivo_modificado_em": arquivo.get("modifiedTime", ""),
            "data_revisao": revisado_em,
        }
        for exemplo in exemplos
    ]


@task
def ingere_revisoes(campanha: str, pasta_campanha_id: str, catalogo: CatalogoValidado) -> dict:
    """Sincroniza a tabela RAG com os Sheets revisados da pasta da campanha (ver
    docstring do módulo). Devolve um resumo pra log/relatório."""
    drive, sheets, client = get_drive_service(), get_sheets_service(), get_bq_client()
    arquivos = [
        a
        for a in lista_recursivo(drive, pasta_campanha_id, MARCADOR_REVISADA, mime_type=DRIVE_SHEET_MIME)
        if not any(pasta.endswith(SUFIXO_PASTA_RAPIDO) for pasta in a["caminho"])
    ]
    _garante_tabela(client)
    plano = planeja_sincronizacao(arquivos, _estado_campanha(client, campanha), EMBEDDING_MODEL)

    novas = [
        linha for arquivo in plano.reprocessar for linha in _linhas_do_arquivo(sheets, arquivo, campanha, catalogo)
    ]
    df_novas = pd.DataFrame(novas)
    if not df_novas.empty:
        embeddings = gera_embeddings_conversas(df_novas["conversa"].tolist())
        df_novas["embedding"] = [vetor.astype(float).tolist() for vetor in embeddings]
        df_novas["modelo_embedding"] = EMBEDDING_MODEL
        df_novas["ingerido_em"] = datetime.now(timezone.utc)

    # Remove antes de inserir: os reprocessados saem e entram de novo com o conteúdo
    # atual. Se o insert falhar, o arquivo fica fora do estado e volta na próxima.
    _remove_arquivos(client, campanha, plano.remover + [a["id"] for a in plano.reprocessar])
    _insere(client, df_novas)

    resumo = {
        "arquivos_revisados": len(arquivos),
        "arquivos_reprocessados": len(plano.reprocessar),
        "arquivos_inalterados": plano.inalterados,
        "arquivos_removidos": len(plano.remover),
        "exemplos_gravados": len(df_novas),
    }
    log(f"[RAG] {campanha}: {resumo}")
    return resumo


@dataclass
class BancoExemplos:
    """Exemplos válidos da campanha + matriz de embeddings alinhada (linha i do df =
    linha i da matriz) — entrada da busca top-k na classificação."""

    exemplos: pd.DataFrame
    embeddings: np.ndarray

    @property
    def vazio(self) -> bool:
        return self.exemplos.empty


@task
def carrega_exemplos(campanha: str, catalogo: CatalogoValidado) -> BancoExemplos:
    """Exemplos revisados da campanha embedados com o modelo atual, filtrados pelo
    Catálogo atual (ver seleciona_exemplos). Banco vazio = classificação zero-shot."""
    client = get_bq_client()
    _garante_tabela(client)
    sql = f"""
        select id_sessao, conversa, categorias, justificativa_revisao, data_revisao, ingerido_em, embedding
        from `{RAG_TABLE}`
        where nome_campanha = @campanha and modelo_embedding = @modelo
    """
    config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("campanha", "STRING", campanha),
            bigquery.ScalarQueryParameter("modelo", "STRING", EMBEDDING_MODEL),
        ]
    )
    bruto = client.query(sql, job_config=config).to_dataframe()
    df = seleciona_exemplos(bruto, catalogo)
    matriz = (
        np.vstack(df["embedding"].map(np.asarray).tolist()).astype(np.float32) if not df.empty else np.empty((0, 0))
    )
    por_categoria = Counter(c for cats in df["categorias"] for c in cats) if not df.empty else Counter()
    log(
        f"[RAG] {campanha}: {len(df)} exemplo(s) válido(s) de {len(bruto)} na tabela"
        + (f" — por categoria: {dict(por_categoria.most_common())}" if por_categoria else " — classificação zero-shot.")
    )
    return BancoExemplos(df.drop(columns=["embedding"]) if not df.empty else df, matriz)
