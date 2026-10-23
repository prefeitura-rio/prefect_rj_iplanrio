"""Comparação de colunas, partições, índices, sinônimos e permissões entre BigQuery e Oracle."""

import re

from pipelines.rj_smfp__nota_carioca_bq_to_oracle.helpers.columns import OracleColumn
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.helpers.structure_models import (
    USABLE_INDEX_STATUSES,
    IndexDefinition,
    Partitioning,
    TablePartition,
)

STATUS_OK = "OK"


STATUS_DIVERGE = "DIVERGE"


DATE_BOUND_PATTERN = re.compile(r"^TO_DATE\('\s*([0-9-]+ [0-9:]+)'")


def describe_column(column: OracleColumn | None) -> str:
    """Descreve tipo e nulidade de uma coluna como no ``CREATE TABLE``.

    :param column: Coluna, ou ``None`` se ausente.
    :returns: Por exemplo ``VARCHAR2(14 BYTE) NOT NULL``, ou ``(ausente)``.
    """
    if column is None:
        return "(ausente)"
    try:
        ddl_type = column.ddl_type
    except NotImplementedError:
        ddl_type = column.data_type
    return ddl_type + ("" if column.nullable else " NOT NULL")


def compare_columns(
    template: list[OracleColumn], actual: list[OracleColumn], bq_types: dict[str, str]
) -> list[list[str]]:
    """Compara, posição a posição, a tabela original com a tabela carregada.

    Nome, ordem, tipo, tamanho e ``NOT NULL`` precisam ser iguais.

    :param template: Colunas da tabela original, na ordem dela.
    :param actual: Colunas da tabela carregada, na ordem dela.
    :param bq_types: Tipo no BigQuery de cada coluna, pelo nome em maiúsculas.
    :returns: Linhas ``[#, coluna, tipo BigQuery, original, carregada, status]``.
    """
    rows = []
    for index in range(max(len(template), len(actual))):
        original = template[index] if index < len(template) else None
        loaded = actual[index] if index < len(actual) else None
        name = original.name if original else "(ausente)"
        same_name = original is not None and loaded is not None and original.name == loaded.name
        same_definition = same_name and describe_column(original) == describe_column(loaded)
        rows.append(
            [
                str(index + 1),
                name if same_name or loaded is None else f"{name} ≠ {loaded.name}",
                bq_types.get(name, "-"),
                describe_column(original),
                describe_column(loaded),
                STATUS_OK if same_definition else STATUS_DIVERGE,
            ]
        )
    return rows


def short_bound(high_value: str) -> str:
    """Encurta o limite de uma partição por data para exibição.

    :param high_value: Limite no texto do dicionário.
    :returns: ``< AAAA-MM-DD HH:MI:SS`` para limites ``TO_DATE``; o texto
        original nos demais casos.
    """
    match = DATE_BOUND_PATTERN.match(high_value)
    return f"< {match.group(1)}" if match else high_value


def describe_partition(partition: TablePartition | None) -> str:
    """Descreve limite e tablespace de uma partição.

    :param partition: Partição, ou ``None`` se ausente.
    :returns: Por exemplo ``< 2026-02-01 00:00:00 em DFEN_BIG_DATA``, ou ``(ausente)``.
    """
    if partition is None:
        return "(ausente)"
    return short_bound(partition.high_value) + (f" em {partition.tablespace}" if partition.tablespace else "")


def compare_partitions(expected: Partitioning | None, actual: Partitioning | None) -> list[list[str]]:
    """Compara o particionamento da tabela original com o da tabela carregada.

    A primeira linha compara tipo, chave e intervalo; as seguintes, cada partição
    declarada, pelo nome, na ordem da original. Partições criadas pelo
    ``INTERVAL`` não entram.

    :param expected: Particionamento da original, ou ``None``.
    :param actual: Particionamento da carregada, ou ``None``.
    :returns: Linhas ``[#, partição, original, carregada, status]``.
    """
    expected_summary = expected.summary if expected else "sem partição"
    actual_summary = actual.summary if actual else "sem partição"
    status = STATUS_OK if expected_summary == actual_summary else STATUS_DIVERGE
    rows = [["-", "(particionamento)", expected_summary, actual_summary, status]]
    wanted = list(expected.partitions) if expected else []
    found = {partition.name: partition for partition in actual.partitions} if actual else {}
    for position, partition in enumerate(wanted, start=1):
        loaded = found.pop(partition.name, None)
        status = STATUS_OK if loaded == partition else STATUS_DIVERGE
        rows.append([str(position), partition.name, describe_partition(partition), describe_partition(loaded), status])
    rows += [
        ["+", name, "(ausente)", describe_partition(partition), STATUS_DIVERGE] for name, partition in found.items()
    ]
    return rows


def compare_indexes(expected: list[IndexDefinition], actual: list[IndexDefinition]) -> list[list[str]]:
    """Compara os índices esperados (os da original, renomeados) com os da tabela carregada.

    Além da definição, o índice precisa estar utilizável e sem grau de
    paralelismo gravado.

    :param expected: Índices esperados, já com o nome da tabela carregada.
    :param actual: Índices da tabela carregada.
    :returns: Linhas ``[índice, esperado, carregado, paralelismo, status do
        índice, resultado]``.
    """
    found = {index.name: index for index in actual}
    rows = []
    for index in expected:
        loaded = found.pop(index.name, None)
        healthy = loaded is not None and loaded.degree == "1" and loaded.status in USABLE_INDEX_STATUSES
        rows.append(
            [
                index.name,
                index.description,
                loaded.description if loaded else "(ausente)",
                loaded.degree if loaded else "-",
                loaded.status if loaded else "-",
                STATUS_OK if healthy and loaded == index else STATUS_DIVERGE,
            ]
        )
    rows += [
        [name, "(não esperado)", index.description, index.degree, index.status, STATUS_DIVERGE]
        for name, index in found.items()
    ]
    return rows


def compare_synonyms(owners: tuple[str, ...], targets: dict[str, str], active: str | None) -> list[list[str]]:
    """Confere se todos os sinônimos da tabela apontam para a tabela física em uso.

    :param owners: Donos esperados dos sinônimos.
    :param targets: Objeto apontado (``dono.tabela``) pelo sinônimo de cada dono.
    :param active: Tabela em uso (``dono.tabela``), ou ``None`` se não houver.
    :returns: Linhas ``[sinônimo, aponta para, status]``.
    """
    rows = []
    for owner in owners:
        target = targets.get(owner)
        status = STATUS_OK if target is not None and target == active else STATUS_DIVERGE
        rows.append([owner, target or "(ausente)", status])
    return rows


def compare_grants(expected: tuple[tuple[str, str], ...], found: list[tuple[str, str]]) -> list[list[str]]:
    """Confere os privilégios concedidos aos consumidores na tabela física.

    :param expected: Pares ``(grantee, privilégios separados por vírgula)``.
    :param found: Pares ``(grantee, privilégio)`` concedidos na tabela.
    :returns: Linhas ``[grantee, esperado, concedido, status]``; privilégios a
        mais não contam como divergência.
    """
    granted: dict[str, set[str]] = {}
    for grantee, privilege in found:
        granted.setdefault(grantee, set()).add(privilege)
    rows = []
    for grantee, privileges in expected:
        wanted = {privilege.strip() for privilege in privileges.split(",")}
        has = granted.get(grantee, set())
        rows.append(
            [
                grantee,
                ", ".join(sorted(wanted)),
                ", ".join(sorted(has)) or "(nenhum)",
                STATUS_OK if wanted <= has else STATUS_DIVERGE,
            ]
        )
    return rows
