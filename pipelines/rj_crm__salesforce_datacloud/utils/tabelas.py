# -*- coding: utf-8 -*-
"""
Carrega e valida tabelas.yaml — a configuração das tabelas do pipeline (o
que cada campo significa está documentado no próprio YAML).

A validação roda no import: campo desconhecido, obrigatório faltando, tipo
errado, source inválido, arquivo de query inexistente ou nome repetido dão
erro aqui, antes de qualquer run começar.
"""

from __future__ import annotations

from dataclasses import dataclass, field, fields
from pathlib import Path
from typing import Literal

import yaml

_DIR = Path(__file__).parent.parent  # raiz do pipeline (tabelas.yaml, sql/)
_SOURCES = ("data_cloud", "crm_rest")


@dataclass(frozen=True)
class Tabela:
    """Uma entrada de tabelas.yaml (padrões iguais aos documentados lá)."""

    nome: str
    grupo: str
    query: str
    source: str = "data_cloud"
    date_columns: list[str] = field(default_factory=list)
    primary_key: str = "id"
    order_by_col: str = "ssot__Id__c"
    critica: bool = False
    janela_utc: bool = False
    output_value_text_action_step_only: bool = False

    @property
    def staging(self) -> str:
        return f"{self.nome}_staging"


def _carrega(caminho: Path) -> list[Tabela]:
    itens = yaml.safe_load(caminho.read_text())
    if not isinstance(itens, list) or not itens:
        raise ValueError(f"{caminho.name}: esperava uma lista de tabelas.")

    campos = {f.name: f for f in fields(Tabela)}
    tipos = {"str": str, "bool": bool, "list[str]": list}
    tabelas: list[Tabela] = []
    for i, item in enumerate(itens, start=1):
        onde = f"{caminho.name}, item {i} ({item.get('nome', '?') if isinstance(item, dict) else '?'})"
        if not isinstance(item, dict):
            raise ValueError(f"{onde}: esperava um objeto com nome/grupo/query/...")
        desconhecidos = set(item) - set(campos)
        if desconhecidos:
            raise ValueError(f"{onde}: campo(s) desconhecido(s) {sorted(desconhecidos)}. Válidos: {sorted(campos)}")
        for nome, valor in item.items():
            esperado = tipos[campos[nome].type]
            if not isinstance(valor, esperado):
                raise ValueError(f"{onde}: '{nome}' deveria ser {campos[nome].type}, veio {valor!r}")
        try:
            tabela = Tabela(**item)
        except TypeError as exc:  # obrigatório faltando
            raise ValueError(f"{onde}: {exc}") from exc
        if tabela.source not in _SOURCES:
            raise ValueError(f"{onde}: source '{tabela.source}' inválido. Use {list(_SOURCES)}.")
        if not (_DIR / "sql" / tabela.query).is_file():
            raise ValueError(f"{onde}: query sql/{tabela.query} não existe.")
        tabelas.append(tabela)

    repetidos = {t.nome for t in tabelas if [x.nome for x in tabelas].count(t.nome) > 1}
    if repetidos:
        raise ValueError(f"{caminho.name}: nome(s) repetido(s) {sorted(repetidos)}.")
    return tabelas


TABELAS: list[Tabela] = _carrega(_DIR / "tabelas.yaml")

# Tipo do parâmetro `tabelas` do flow — vira seleção múltipla na UI do Prefect
# (enum no schema) e o Prefect já rejeita nome inválido antes do run começar.
NomeTabela = Literal[tuple(t.nome for t in TABELAS)]
