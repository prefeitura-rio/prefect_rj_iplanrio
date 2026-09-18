# -*- coding: utf-8 -*-
"""Monta o relatório (.docx + 2 csv complementares) e publica no Google Drive.
Idempotência mora aqui: `ja_existe_relatorio` confere, pelo prefixo determinístico do
nome de arquivo, se já existe relatório pra esse HSM + data de geração."""

from __future__ import annotations

import io
import math
from dataclasses import dataclass, field
from datetime import date, datetime

import pandas as pd
from docx import Document
from docx.oxml import OxmlElement
from docx.oxml.ns import qn
from docx.shared import Pt, RGBColor
from iplanrio.pipelines_utils.logging import log
from prefect import task

from pipelines.rj_crm__relatorio_engajamento_hsm.config import CHATBOT_DATASET, CHATBOT_TABLE, CSV_SEP, PROJECT_ID
from pipelines.rj_crm__relatorio_engajamento_hsm.tasks.extract import sql_query_utilizada
from pipelines.rj_crm__relatorio_engajamento_hsm.utils.drive import (
    busca_pasta,
    confirma_pasta_raiz,
    get_drive_service,
    pasta_hsm_id,
    upload_bytes,
)
from pipelines.rj_crm__relatorio_engajamento_hsm.utils.texto import hsm_sane

_Z_IC_95 = 1.959963985  # z crítico do intervalo de confiança de 95%


def _margem_erro_wilson(p: float, n: int, N: int | None = None, z: float = _Z_IC_95) -> float:
    """Meia-largura do intervalo de Wilson (95%) pra uma proporção binomial p estimada
    com n observações, com correção de população finita (N = tamanho da categoria nesse
    disparo): sem ela, uma categoria pequena julgada 100% (n == N, censo completo, erro
    amostral zero por definição) apareceria com margem de erro > 0, o que é errado."""
    if n <= 0:
        return float("nan")
    if N is not None and n >= N:
        return 0.0
    denom = 1 + z**2 / n
    margem = (z * math.sqrt(p * (1 - p) / n + z**2 / (4 * n**2))) / denom
    if N is not None and N > 1:
        margem *= math.sqrt((N - n) / (N - 1))
    return margem


@dataclass
class Exemplo:
    conversa: str
    resumo: str


@dataclass
class Categoria:
    nome: str
    descricao: str
    qtd: int
    pct: float
    n_avaliadas: int
    n_corretas: int
    exemplos: list[Exemplo] = field(default_factory=list)

    @property
    def precisao(self) -> float | None:
        return self.n_corretas / self.n_avaliadas if self.n_avaliadas else None

    @property
    def margem_erro(self) -> float | None:
        if not self.n_avaliadas or self.precisao is None:
            return None
        return _margem_erro_wilson(self.precisao, self.n_avaliadas, self.qtd)


def _monta_categorias(df: pd.DataFrame, catalogo: dict[str, str], n_exemplos_por_categoria: int) -> list[Categoria]:
    total = len(df)
    categorias = []
    for nome, grupo in df.groupby("categoria"):
        julgadas = grupo[grupo["juiz_veredito"].astype(str).str.strip() != ""]
        n_corretas = int((julgadas["juiz_veredito"] == "CORRETO").sum())
        amostra_exemplos = grupo[grupo["conversa_completa"].notna()].head(n_exemplos_por_categoria)
        exemplos = [
            Exemplo(conversa=row["conversa_completa"], resumo=str(row.get("resumo_gerado") or "").strip())
            for _, row in amostra_exemplos.iterrows()
        ]
        categorias.append(
            Categoria(
                nome=nome,
                descricao=catalogo.get(nome, ""),
                qtd=len(grupo),
                pct=100 * len(grupo) / total if total else 0.0,
                n_avaliadas=len(julgadas),
                n_corretas=n_corretas,
                exemplos=exemplos,
            )
        )
    categorias.sort(key=lambda c: c.qtd, reverse=True)
    return categorias


# ============================================================================
# Geração do .docx
# ============================================================================

_COR_ACCENT = RGBColor(0xB5, 0x50, 0x2E)
_COR_FUNDO_CODIGO = "FBE9E2"
_COR_BORDA_CODIGO = "B5502E"
_MAX_CHARS_EXEMPLO = 1200


def _trunca(texto: str, limite: int = _MAX_CHARS_EXEMPLO) -> str:
    texto = texto or ""
    return texto if len(texto) <= limite else texto[:limite].rstrip() + " […]"


def _sombreia_paragrafo(paragraph, cor_fundo: str, cor_borda: str) -> None:
    pPr = paragraph._p.get_or_add_pPr()
    shd = OxmlElement("w:shd")
    shd.set(qn("w:val"), "clear")
    shd.set(qn("w:color"), "auto")
    shd.set(qn("w:fill"), cor_fundo)
    pPr.append(shd)
    pBdr = OxmlElement("w:pBdr")
    for lado in ("top", "left", "bottom", "right"):
        borda = OxmlElement(f"w:{lado}")
        borda.set(qn("w:val"), "single")
        borda.set(qn("w:sz"), "4")
        borda.set(qn("w:space"), "4")
        borda.set(qn("w:color"), cor_borda)
        pBdr.append(borda)
    pPr.append(pBdr)


def _adiciona_bloco_codigo(doc: Document, texto: str) -> None:
    p = doc.add_paragraph()
    linhas = texto.split("\n")
    for i, linha in enumerate(linhas):
        run = p.add_run(linha if linha.strip() else " ")
        run.font.name = "Courier New"
        run.font.size = Pt(9)
        if i < len(linhas) - 1:
            run.add_break()
    _sombreia_paragrafo(p, _COR_FUNDO_CODIGO, _COR_BORDA_CODIGO)


def _gera_docx(
    nome_hsm: str,
    nome_campanha: str | None,
    nome_eixo: str | None,
    categorias: list[Categoria],
    total_classificadas: int,
    hsm_texto: str,
    total_disparos: int,
    total_engajados: int,
    sql_utilizada: str,
    nomes_arquivos_csv: list[str],
) -> Document:
    doc = Document()
    doc.add_heading(f"Relatório de Engajamento — {nome_hsm}", level=0)

    meta = doc.add_paragraph()
    meta.add_run(f"Campanha: {nome_campanha or '—'}    ·    Eixo: {nome_eixo or '—'}").italic = True
    doc.add_paragraph(f"Gerado em {datetime.now():%d/%m/%Y %H:%M}")

    doc.add_heading("Resumo", level=1)
    doc.add_paragraph(f"Total de disparos: {total_disparos}")
    pct_engajamento = 100 * total_engajados / total_disparos if total_disparos else 0.0
    doc.add_paragraph(f"Sessões com resposta do cidadão: {total_engajados} ({pct_engajamento:.1f}% dos disparos)")
    pct_classificadas = 100 * total_classificadas / total_engajados if total_engajados else 0.0
    doc.add_paragraph(f"Conversas classificadas: {total_classificadas} ({pct_classificadas:.1f}% das respostas)")
    doc.add_paragraph(f"Categorias identificadas: {len(categorias)}")

    n_avaliadas_geral = sum(c.n_avaliadas for c in categorias)
    n_corretas_geral = sum(c.n_corretas for c in categorias)
    if total_classificadas:
        pct_avaliadas = 100 * n_avaliadas_geral / total_classificadas
        doc.add_paragraph(f"Conversas avaliadas pelo juiz: {n_avaliadas_geral} ({pct_avaliadas:.1f}% das classificadas)")
    precisao_geral = doc.add_paragraph()
    precisao_geral.add_run("Precisão média da classificação (todas as categorias): ")
    if n_avaliadas_geral:
        p_geral = n_corretas_geral / n_avaliadas_geral
        margem_geral = _margem_erro_wilson(p_geral, n_avaliadas_geral, total_classificadas)
        precisao_geral.add_run(f"{p_geral:.0%} ± {margem_geral:.0%} (n={n_avaliadas_geral})").bold = True
    else:
        precisao_geral.add_run("não avaliada")

    if hsm_texto:
        doc.add_paragraph().add_run("HSM enviado:").bold = True
        doc.add_paragraph(style="Intense Quote").add_run(_trunca(hsm_texto))

    doc.add_heading("Observações importantes", level=1)
    doc.add_paragraph(
        "1) Este relatório de engajamento é uma análise pontual das respostas de um disparo, as categorias "
        "criadas aqui não são de monitoramento contínuo."
    )
    doc.add_paragraph(
        "2) Este relatório foi gerado a partir de uma LLM (um modelo de linguagem) e portanto pode conter "
        "erros. Revise os números com atenção e se atente às métricas e exemplos fornecidos para não tirar "
        "conclusões erradas."
    )
    nomes_csv_fmt = " e ".join(f"'{n}'" for n in nomes_arquivos_csv)
    doc.add_paragraph(
        f"3) Os arquivos {nomes_csv_fmt}, na mesma pasta deste relatório, trazem o detalhe por categoria e por "
        f"conversa usados aqui. Eles usam ponto e vírgula ({CSV_SEP}) como separador de colunas, não vírgula."
    )

    doc.add_heading("Query utilizada na análise", level=1)
    doc.add_paragraph(
        f"Consulta em {PROJECT_ID}.{CHATBOT_DATASET}.{CHATBOT_TABLE} usada pra extrair as conversas "
        "deste disparo, com os parâmetros já substituídos pelos valores reais desta análise."
    )
    _adiciona_bloco_codigo(doc, sql_utilizada)

    doc.add_heading("Categorias", level=1)
    tabela = doc.add_table(rows=1, cols=4)
    tabela.style = "Light Grid Accent 2"
    for cel, texto in zip(tabela.rows[0].cells, ["Categoria", "Qtd.", "% do engajamento", "Precisão (LLM juiz)"]):
        cel.paragraphs[0].add_run(texto).bold = True
    for cat in categorias:
        linha = tabela.add_row().cells
        linha[0].text = cat.nome
        linha[1].text = str(cat.qtd)
        linha[2].text = f"{cat.pct:.1f}%"
        linha[3].text = (
            f"{cat.precisao:.0%} ± {cat.margem_erro:.0%} (n={cat.n_avaliadas})" if cat.precisao is not None else "não avaliada"
        )
    doc.add_paragraph()

    for cat in categorias:
        h = doc.add_heading(cat.nome, level=2)
        h.runs[0].font.color.rgb = _COR_ACCENT
        doc.add_paragraph(cat.descricao or "(sem descrição registrada no catálogo)")

        resumo = doc.add_paragraph()
        resumo.add_run(f"{cat.qtd} conversa(s) · {cat.pct:.1f}% do engajamento · precisão: ")
        if cat.precisao is not None:
            resumo.add_run(f"{cat.precisao:.0%} ± {cat.margem_erro:.0%} (amostra de {cat.n_avaliadas})").bold = True
        else:
            resumo.add_run("não avaliada")

        if cat.exemplos:
            doc.add_paragraph().add_run("Exemplos:").bold = True
            tabela_ex = doc.add_table(rows=1, cols=2)
            tabela_ex.style = "Light Grid Accent 2"
            for cel, texto in zip(tabela_ex.rows[0].cells, ["Conversa", "Resumo"]):
                cel.paragraphs[0].add_run(texto).bold = True
            for ex in cat.exemplos:
                linha_ex = tabela_ex.add_row().cells
                linha_ex[0].text = _trunca(ex.conversa)
                linha_ex[1].text = _trunca(ex.resumo) if ex.resumo else "(sem resumo)"
            doc.add_paragraph()
        else:
            doc.add_paragraph().add_run("Sem exemplo disponível.").italic = True

    return doc


# ============================================================================
# Idempotência + publicação no Drive
# ============================================================================


def _slug_geracao(nome_hsm: str, data_referencia: date) -> str:
    """Prefixo determinístico (sem timestamp) que identifica o relatório de um HSM pra
    uma data de geração — usado tanto pra checar duplicidade quanto pra nomear os
    arquivos publicados."""
    return f"relatorio_engajamento_{hsm_sane(nome_hsm)}__geracao_{data_referencia.isoformat()}"


@task
def ja_existe_relatorio(nome_hsm: str, data_referencia: date, drive_pasta_raiz_id: str) -> bool:
    """Confere na subpasta do HSM se já existe relatório (.docx) pra essa data de
    geração — é a forma de idempotência pedida: rodar o flow de novo no mesmo dia não
    duplica o relatório; se a data de geração mudar (nova rodada pro mesmo HSM), o
    prefixo muda e um relatório novo é gerado."""
    drive = get_drive_service()
    confirma_pasta_raiz(drive, drive_pasta_raiz_id)
    pasta_id = busca_pasta(drive, nome_hsm, parent_id=drive_pasta_raiz_id)
    if pasta_id is None:
        return False
    prefixo = _slug_geracao(nome_hsm, data_referencia)
    query = f"name contains '{prefixo}' and '{pasta_id}' in parents and trashed = false"
    resultado = drive.files().list(q=query, fields="files(id, name)", spaces="drive").execute()
    return len(resultado.get("files", [])) > 0


@task
def monta_e_publica_relatorio(
    nome_hsm: str,
    data_disparo: date,
    data_referencia: date,
    df: pd.DataFrame,
    catalogo: dict[str, str],
    total_disparos: int,
    total_engajados: int,
    drive_pasta_raiz_id: str,
    n_exemplos_por_categoria: int,
) -> None:
    categorias = _monta_categorias(df, catalogo, n_exemplos_por_categoria)
    nome_campanha = next((v for v in df["nome_campanha"].dropna() if v), None) if "nome_campanha" in df.columns else None
    nome_eixo = next((v for v in df["nome_eixo"].dropna() if v), None) if "nome_eixo" in df.columns else None
    hsm_texto = next((v for v in df["hsm_texto"].dropna() if v), "") if "hsm_texto" in df.columns else ""

    slug = _slug_geracao(nome_hsm, data_referencia)
    nome_csv_categorias = f"categorias_{slug}.csv"
    nome_csv_classificacoes = f"classificacoes_{slug}.csv"

    doc = _gera_docx(
        nome_hsm=nome_hsm,
        nome_campanha=nome_campanha,
        nome_eixo=nome_eixo,
        categorias=categorias,
        total_classificadas=len(df),
        hsm_texto=hsm_texto,
        total_disparos=total_disparos,
        total_engajados=total_engajados,
        sql_utilizada=sql_query_utilizada(nome_hsm, data_disparo),
        nomes_arquivos_csv=[nome_csv_categorias, nome_csv_classificacoes],
    )
    buffer_docx = io.BytesIO()
    doc.save(buffer_docx)

    resumo_categorias = pd.DataFrame(
        [
            {
                "categoria": c.nome,
                "descricao": c.descricao,
                "qtd": c.qtd,
                "pct_do_engajamento": round(c.pct, 1),
                "n_avaliadas_juiz": c.n_avaliadas,
                "precisao": round(c.precisao, 4) if c.precisao is not None else None,
                "margem_erro_wilson_95": round(c.margem_erro, 4) if c.margem_erro is not None else None,
            }
            for c in categorias
        ]
    )
    colunas_detalhe = [
        "id_sessao_48h", "cpf", "telefone", "categoria", "categoria_justificativa",
        "resumo_gerado", "juiz_veredito", "juiz_categoria_esperada", "juiz_justificativa",
    ]
    df_detalhe = df.reindex(columns=[c for c in colunas_detalhe if c in df.columns])

    drive = get_drive_service()
    confirma_pasta_raiz(drive, drive_pasta_raiz_id)
    pasta_id = pasta_hsm_id(drive, drive_pasta_raiz_id, nome_hsm)

    upload_bytes(
        drive, pasta_id, f"{slug}.docx", buffer_docx.getvalue(),
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    )
    upload_bytes(
        drive, pasta_id, nome_csv_categorias,
        resumo_categorias.to_csv(index=False, sep=CSV_SEP).encode("utf-8"), "text/csv",
    )
    upload_bytes(
        drive, pasta_id, nome_csv_classificacoes,
        df_detalhe.to_csv(index=False, sep=CSV_SEP).encode("utf-8"), "text/csv",
    )
    log(f"[FLOW] {nome_hsm}: relatório da geração {data_referencia} publicado no Drive (subpasta '{nome_hsm}').")
