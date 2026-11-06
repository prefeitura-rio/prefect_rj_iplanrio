"""Carrega e valida ``tabelas.yaml``.

A validação roda no import: campo desconhecido, tipo errado, DLO faltando em
uma das origens, nome repetido ou renomeação inválida dão erro antes de
qualquer run começar.
"""

from __future__ import annotations

from dataclasses import dataclass, field, fields
from pathlib import Path
from typing import Literal

import yaml

_DIR = Path(__file__).parent.parent  # raiz da pipeline (tabelas.yaml)
ORIGENS = ("qa", "staging", "prod")


@dataclass(frozen=True)
class Tabela:
    """Uma entrada de ``tabelas.yaml``."""

    nome: str
    grupo: str
    dlo: dict[str, str]
    primary_key: str = "id"
    order_by: str = "Id__c"
    coluna_modificacao: str = "SystemModstamp__c"
    renomear: dict[str, str] = field(default_factory=dict)
    booleanas: list[str] = field(default_factory=list)
    cluster: list[str] = field(default_factory=list)

    @property
    def staging(self) -> str:
        """Nome da tabela de staging."""
        return f"{self.nome}_staging"

    def dlo_de(self, origem: str) -> str:
        """DLO desta tabela na org informada.

        :param origem: ``qa``, ``staging`` ou ``prod``.
        :returns: nome da tabela no Data Cloud.
        :raises ValueError: Se a origem não estiver configurada.
        """
        try:
            return self.dlo[origem]
        except KeyError as exc:
            raise ValueError(f"tabela '{self.nome}' sem DLO para origem '{origem}'.") from exc


def _tipo_esperado(anotacao: object) -> type:
    """Tipo de runtime a partir da anotação do dataclass.

    ``from __future__ import annotations`` faz o dataclass guardar a anotação
    como texto (``"dict[str, str]"``); tipos genéricos são resolvidos pelo
    prefixo, então tanto faz se a anotação vier como texto ou como tipo.

    :param anotacao: anotação do campo (texto ou tipo).
    :returns: tipo esperado do valor no YAML.
    """
    nome = anotacao if isinstance(anotacao, str) else getattr(anotacao, "__origin__", anotacao)
    if not isinstance(nome, str):
        return nome
    nome = nome.replace(" ", "")
    if nome.startswith("dict["):
        return dict
    if nome.startswith("list["):
        return list
    return {"str": str, "bool": bool, "int": int, "dict": dict, "list": list}[nome]


def _carrega(caminho: Path) -> list[Tabela]:
    itens = yaml.safe_load(caminho.read_text(encoding="utf-8"))
    if not isinstance(itens, list) or not itens:
        raise ValueError(f"{caminho.name}: esperava uma lista de tabelas.")

    campos = {f.name: f for f in fields(Tabela)}
    tabelas: list[Tabela] = []

    for i, item in enumerate(itens, start=1):
        onde = f"{caminho.name}, item {i}"
        if not isinstance(item, dict):
            raise ValueError(f"{onde}: esperava um objeto com nome/grupo/dlo.")
        onde = f"{onde} ({item.get('nome', '?')})"

        desconhecidos = set(item) - set(campos)
        if desconhecidos:
            raise ValueError(f"{onde}: campo(s) desconhecido(s) {sorted(desconhecidos)}. Válidos: {sorted(campos)}")

        for nome, valor in item.items():
            esperado = _tipo_esperado(campos[nome].type)
            if not isinstance(valor, esperado):
                raise ValueError(f"{onde}: '{nome}' deveria ser {campos[nome].type}, veio {valor!r}")

        try:
            tabela = Tabela(**item)
        except TypeError as exc:
            raise ValueError(f"{onde}: {exc}") from exc

        faltando = set(ORIGENS) - set(tabela.dlo)
        if faltando:
            raise ValueError(f"{onde}: dlo sem a(s) origem(s) {sorted(faltando)}.")
        extras = set(tabela.dlo) - set(ORIGENS)
        if extras:
            raise ValueError(f"{onde}: dlo com origem inválida {sorted(extras)}. Use {list(ORIGENS)}.")
        if tabela.primary_key not in tabela.renomear.values() and tabela.primary_key != "id":
            raise ValueError(f"{onde}: primary_key '{tabela.primary_key}' não parece um nome normalizado.")

        tabelas.append(tabela)

    nomes = [t.nome for t in tabelas]
    repetidos = {n for n in nomes if nomes.count(n) > 1}
    if repetidos:
        raise ValueError(f"{caminho.name}: nome(s) repetido(s) {sorted(repetidos)}.")
    return tabelas


TABELAS: list[Tabela] = _carrega(_DIR / "tabelas.yaml")

# Tipo do parâmetro `tabelas` do flow: vira seleção múltipla na UI do Prefect e
# o Prefect já rejeita nome inválido antes do run começar.
NomeTabela = Literal[tuple(t.nome for t in TABELAS)]

Origem = Literal[ORIGENS]
