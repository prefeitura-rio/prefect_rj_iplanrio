# -*- coding: utf-8 -*-
"""Catálogo de categorias.

CatalogoCategorias — em memória: usado pela descoberta (só ela registra categoria nova)
e como conjunto de categorias ativas pra classificação (só busca, nunca cria).

Catálogo validado — o Sheet `Catálogo` da pasta da campanha, preenchido pela flow na
Fase 1 e validado pela secretaria (coluna Decisão: Manter / Excluir / Nova). O flow só
LÊ esse Sheet (interpreta_catalogo / le_catalogo), nunca altera."""

from __future__ import annotations

from dataclasses import dataclass, field

import pandas as pd
from googleapiclient.discovery import Resource

from pipelines.rj_crm__relatorio_engajamento_hsm.config import (
    ARQUIVO_CATALOGO,
    COL_CAT_CATEGORIA,
    COL_CAT_DATA_DECISAO,
    COL_CAT_DECISAO,
    COL_CAT_DESCRICAO,
    DECISOES_ATIVAS,
    DECISOES_VALIDAS,
    DRIVE_SHEET_MIME,
)
from pipelines.rj_crm__relatorio_engajamento_hsm.utils.drive import busca_arquivo
from pipelines.rj_crm__relatorio_engajamento_hsm.utils.sheets import le_sheet
from pipelines.rj_crm__relatorio_engajamento_hsm.utils.texto import normaliza_rotulo


class CatalogoCategorias:
    def __init__(self):
        self._por_chave: dict[str, tuple[str, str]] = {}

    def registra(self, nome: str, descricao: str = "") -> str:
        """Registra (se nova) e retorna o nome CANÔNICO — usado como valor gravado,
        mesmo se a LLM mandou uma grafia levemente diferente. Só a descoberta chama
        isso — é a única que pode criar categoria."""
        chave = normaliza_rotulo(nome)
        if chave not in self._por_chave:
            self._por_chave[chave] = (nome.strip(), (descricao or "").strip())
        return self._por_chave[chave][0]

    def busca(self, nome: str) -> str | None:
        """Como `registra`, mas NUNCA cria — devolve o nome canônico se a categoria já
        existe, ou None. Usado pela classificação: ela só pode escolher entre
        categorias já existentes, criar é trabalho exclusivo da descoberta."""
        entrada = self._por_chave.get(normaliza_rotulo(nome))
        return entrada[0] if entrada else None

    def como_dict(self) -> dict[str, str]:
        return dict(self._por_chave.values())

    def texto_prompt(self) -> str:
        if not self._por_chave:
            return "(nenhuma categoria criada ainda — você pode criar as primeiras)"
        linhas = sorted(f"- {nome}: {descricao}" for nome, descricao in self._por_chave.values())
        return "\n".join(linhas)


# ============================================================================
# Catálogo validado (Sheet da campanha)
# ============================================================================

# Data da decisão não é obrigatória no cabeçalho: é só auditoria (ver CatalogoValidado).
_COLUNAS_CATALOGO = (COL_CAT_CATEGORIA, COL_CAT_DESCRICAO, COL_CAT_DECISAO)
_DECISOES_POR_CHAVE = {d.casefold(): d for d in DECISOES_VALIDAS}


@dataclass
class CatalogoValidado:
    """`ativas`: decisão Manter/Nova — as únicas que a classificação pode usar e que um
    exemplo revisado pode ter. `excluidas`: decisão Excluir. `pendentes`: sem decisão
    (ou com valor que não é Manter/Excluir/Nova) — ficam de fora; se houver ativas, a
    rodada segue e avisa no status da solicitação (ver observacao).

    A data da decisão é só de auditoria: NÃO influencia nada (categoria ativa sem data
    vale normalmente — `ativas_sem_data` só pro log).

    `todas`: toda categoria do Sheet, qualquer decisão — ponto de partida da descoberta
    de sugestões no relatório (pra não sugerir de novo o que já está no Sheet, inclusive
    o que a secretaria excluiu ou ainda não decidiu)."""

    ativas: CatalogoCategorias = field(default_factory=CatalogoCategorias)
    todas: CatalogoCategorias = field(default_factory=CatalogoCategorias)
    excluidas: list[str] = field(default_factory=list)
    pendentes: list[str] = field(default_factory=list)
    ativas_sem_data: list[str] = field(default_factory=list)
    link: str = ""

    @property
    def vazio(self) -> bool:
        return not self.ativas.como_dict()

    def resumo(self) -> str:
        return (
            f"{len(self.ativas.como_dict())} ativa(s), {len(self.excluidas)} excluída(s), "
            f"{len(self.pendentes)} sem decisão, {len(self.ativas_sem_data)} ativa(s) sem data da decisão"
        )

    def observacao(self) -> str:
        """Aviso pro status da solicitação quando o Catálogo foi validado pela metade
        ("" se não há nada a avisar)."""
        if not self.pendentes:
            return ""
        return f"{len(self.pendentes)} categoria(s) sem decisão no Catálogo foram ignoradas"


def interpreta_catalogo(df: pd.DataFrame) -> CatalogoValidado:
    """Sheet do Catálogo (DataFrame de strings, como le_sheet devolve) -> catálogo
    validado. Decisão comparada sem diferenciar maiúscula ('manter' vale). ValueError
    se faltar coluna — Catálogo com cabeçalho mexido não pode ser lido pela metade."""
    faltando = [c for c in _COLUNAS_CATALOGO if c not in df.columns]
    if faltando:
        raise ValueError(f"Catálogo sem a(s) coluna(s) {faltando}.")

    catalogo = CatalogoValidado()
    for _, linha in df.iterrows():
        nome = str(linha[COL_CAT_CATEGORIA]).strip()
        if not nome:
            continue
        catalogo.todas.registra(nome, str(linha[COL_CAT_DESCRICAO]))
        decisao = _DECISOES_POR_CHAVE.get(str(linha[COL_CAT_DECISAO]).strip().casefold())
        if decisao in DECISOES_ATIVAS:
            catalogo.ativas.registra(nome, str(linha[COL_CAT_DESCRICAO]))
            if not str(linha.get(COL_CAT_DATA_DECISAO, "")).strip():
                catalogo.ativas_sem_data.append(nome)
        elif decisao == "Excluir":
            catalogo.excluidas.append(nome)
        else:
            catalogo.pendentes.append(nome)
    return catalogo


def le_catalogo(drive: Resource, sheets: Resource, pasta_campanha_id: str) -> CatalogoValidado | None:
    """Catálogo validado da campanha, ou None se não existir um Sheet chamado
    exatamente ARQUIVO_CATALOGO na pasta dela ('Catálogo_old' não conta)."""
    arquivo = busca_arquivo(drive, ARQUIVO_CATALOGO, pasta_campanha_id, mime_type=DRIVE_SHEET_MIME)
    if arquivo is None:
        return None
    catalogo = interpreta_catalogo(le_sheet(sheets, arquivo["id"]))
    catalogo.link = arquivo.get("webViewLink", "")
    return catalogo
