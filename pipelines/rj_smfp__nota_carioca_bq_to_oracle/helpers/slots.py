"""Tabelas físicas A e B atrás de sinônimos: a carga vai para a inativa e a troca é só repontar os sinônimos."""

import re
from dataclasses import dataclass
from datetime import UTC, datetime

from pipelines.rj_smfp__nota_carioca_bq_to_oracle.helpers.oracle import (
    MANAGED_TABLE_MARKER,
    validate_identifier,
)

SLOTS = ("A", "B")


CONSUMER_SYNONYM_OWNERS = ("NFSE_SIGA", "NFSE_USER", "NFSE_OWNER")


CONSUMER_GRANTS = (
    ("RL_NFSE", "SELECT"),
    ("RL_NFSE_SIGA", "SELECT"),
    ("RL_NFSEOWNER_DRL", "SELECT"),
    ("NFSE_OWNER", "SELECT, ALTER, DELETE"),
)


# Tabela particionada aparece em all_objects também como TABLE PARTITION/SUBPARTITION,
# com o nome da tabela.
REPLACEABLE_OBJECT_TYPES = {"TABLE", "TABLE PARTITION", "TABLE SUBPARTITION", "SYNONYM"}


SNAPSHOT_PATTERN = re.compile(r"snapshot do BigQuery de ([0-9-]+ [0-9:]+) UTC; carregada em ([0-9-]+ [0-9:]+) UTC")


TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"


@dataclass(frozen=True)
class SynonymTarget:
    """Sinônimo e o objeto para o qual ele aponta, como em ``all_synonyms``.

    :param owner: Dono do sinônimo.
    :param table_owner: Dono do objeto apontado.
    :param table_name: Nome do objeto apontado.
    """

    owner: str
    table_owner: str
    table_name: str


@dataclass(frozen=True)
class SlotPlan:
    """Qual tabela física está em uso e qual recebe a carga.

    :param base: Nome estável que a aplicação usa (``BQLOAD_<tabela>``), dado aos
        sinônimos.
    :param active: Tabela física para a qual os sinônimos apontam, ou ``None``
        antes da primeira troca.
    :param inactive: Tabela física que recebe a carga.
    """

    base: str
    active: str | None
    inactive: str

    @property
    def slot(self) -> str:
        """Retorna o sufixo (``A`` ou ``B``) da tabela que recebe a carga."""
        return self.inactive.rsplit("_", 1)[1]


def slot_table_name(base: str, slot: str) -> str:
    """Retorna o nome de uma das tabelas físicas.

    :param base: Nome estável da tabela (``BQLOAD_<tabela>``).
    :param slot: ``A`` ou ``B``.
    :returns: ``<base>_<slot>``.
    :raises ValueError: Se o nome não for um identificador válido.
    """
    return validate_identifier(f"{base}_{slot}")


def synonym_owners(schema: str) -> tuple[str, ...]:
    """Lista os donos dos sinônimos da tabela, começando pelo schema de destino.

    :param schema: Schema das tabelas físicas.
    :returns: Schema de destino e schemas dos consumidores.
    """
    return (schema, *CONSUMER_SYNONYM_OWNERS)


def choose_slots(base: str, schema: str, synonyms: list[SynonymTarget]) -> SlotPlan:
    """Define a tabela em uso e a que recebe a carga a partir dos sinônimos.

    A tabela em uso é a apontada pelo sinônimo do schema de destino ou, se ele
    ainda não existir (troca interrompida), pela primeira tabela física apontada
    por um sinônimo dos consumidores.

    :param base: Nome estável da tabela.
    :param schema: Schema das tabelas físicas.
    :param synonyms: Sinônimos com o nome ``base``, de qualquer dono.
    :returns: Tabela em uso e tabela que recebe a carga.
    :raises PermissionError: Se algum sinônimo da pipeline apontar para um objeto
        que ela não gerencia.
    """
    slot_tables = {slot_table_name(base, slot): slot for slot in SLOTS}
    owners = synonym_owners(schema)
    ours = [synonym for synonym in synonyms if synonym.owner in owners]
    for synonym in ours:
        if synonym.table_owner != schema or (synonym.table_name not in slot_tables and synonym.table_name != base):
            raise PermissionError(
                f"O sinônimo {synonym.owner}.{base} aponta para {synonym.table_owner}.{synonym.table_name}, "
                + "que a pipeline não gerencia. Nada foi alterado."
            )
    by_owner = {synonym.owner: synonym.table_name for synonym in ours}
    active = next((by_owner[owner] for owner in owners if by_owner.get(owner) in slot_tables), None)
    inactive_slot = "B" if active is not None and slot_tables[active] == "A" else "A"
    return SlotPlan(base=base, active=active, inactive=slot_table_name(base, inactive_slot))


def synonyms_to_realign(plan: SlotPlan, synonyms: list[SynonymTarget]) -> list[str]:
    """Lista os donos de sinônimos que apontam para a tabela que vai receber a carga.

    Acontece quando uma troca foi interrompida no meio. Esses sinônimos voltam
    para a tabela em uso antes da carga, para ninguém ler uma tabela vazia.

    :param plan: Tabela em uso e tabela que recebe a carga.
    :param synonyms: Sinônimos da pipeline.
    :returns: Donos dos sinônimos a repontar, na ordem recebida.
    """
    if plan.active is None:
        return []
    return [synonym.owner for synonym in synonyms if synonym.table_name == plan.inactive]


def load_comment(source: str, snapshot_modified: datetime, loaded_at: datetime) -> str:
    """Monta o comentário da tabela física após uma carga completa.

    :param source: Tabela de origem no BigQuery.
    :param snapshot_modified: Última alteração do BigQuery na foto carregada.
    :param loaded_at: Fim da carga.
    :returns: Comentário com a marca da pipeline, a origem, a foto e o horário.
    """
    return (
        f"{MANAGED_TABLE_MARKER}: carga a partir de {source}; "
        + f"snapshot do BigQuery de {snapshot_modified.astimezone(UTC):{TIMESTAMP_FORMAT}} UTC; "
        + f"carregada em {loaded_at.astimezone(UTC):{TIMESTAMP_FORMAT}} UTC"
    )


def parse_load_comment(comment: str | None) -> tuple[str, str] | None:
    """Extrai do comentário a foto do BigQuery carregada e o horário da carga.

    :param comment: Comentário da tabela física.
    :returns: ``(foto do BigQuery, horário da carga)`` em UTC, ou ``None`` se a
        tabela ainda não terminou uma carga.
    """
    match = SNAPSHOT_PATTERN.search(comment or "")
    return (match.group(1), match.group(2)) if match else None
