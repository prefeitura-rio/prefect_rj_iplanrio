"""The A and B physical tables behind synonyms.

The load goes to the idle one, and the swap only points the synonyms again.
"""

from datetime import UTC, datetime
from re import compile as compile_pattern

from pipelines.rj_smfp__nota_carioca_bq_to_oracle.constants import CONSUMER_SYNONYM_OWNERS, MANAGED_TABLE_MARKER
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.helpers.oracle import validate_identifier
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.types import SlotPlan, SynonymTarget

SLOTS = ("A", "B")


SNAPSHOT_PATTERN = compile_pattern(r"snapshot do BigQuery de ([0-9-]+ [0-9:]+) UTC; carregada em ([0-9-]+ [0-9:]+) UTC")


TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"


def slot_table_name(base: str, slot: str) -> str:
    """Return the name of one of the physical tables, `<base>_<slot>`.

    `base` is the stable name of the table (`BQLOAD_<table>`) and `slot` is `A` or `B`.
    Raise `ValueError` if the name is not a valid identifier.
    """
    return validate_identifier(f"{base}_{slot}")


def synonym_owners(schema: str) -> tuple[str, ...]:
    """List the owners of the synonyms of the table, and start with the target schema.

    Return the target schema and the schemas of the consumers.
    """
    return (schema, *CONSUMER_SYNONYM_OWNERS)


def choose_slots(base: str, schema: str, synonyms: list[SynonymTarget]) -> SlotPlan:
    """Choose the table in use and the one that receives the load, from the synonyms.

    The table in use is the one that the synonym of the target schema points to. If that
    synonym does not exist yet (an interrupted swap), it is the first physical table
    that a synonym of the consumers points to. `synonyms` have the name `base`, of any
    owner. Raise `PermissionError` if a synonym of the pipeline points to an object that
    it does not manage.
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
    """List the owners of synonyms that point to the table that will load.

    It happens when a swap stopped in the middle. These synonyms go back to the table in
    use before the load, so nobody reads an empty table. Return the owners in the order
    they came.
    """
    if plan.active is None:
        return []
    return [synonym.owner for synonym in synonyms if synonym.table_name == plan.inactive]


def load_comment(source: str, snapshot_modified: datetime, loaded_at: datetime) -> str:
    """Build the comment of the physical table after a full load.

    It has the mark of the pipeline, the source, the snapshot and the time.
    `snapshot_modified` is the last change of BigQuery in the loaded snapshot, and
    `loaded_at` is the end of the load.
    """
    return (
        f"{MANAGED_TABLE_MARKER}: carga a partir de {source}; "
        + f"snapshot do BigQuery de {snapshot_modified.astimezone(UTC):{TIMESTAMP_FORMAT}} UTC; "
        + f"carregada em {loaded_at.astimezone(UTC):{TIMESTAMP_FORMAT}} UTC"
    )


def parse_load_comment(comment: str | None) -> tuple[str, str] | None:
    """Read the loaded BigQuery snapshot and the load time from the comment.

    Return `(BigQuery snapshot, load time)` in UTC, or `None` if the table has not
    finished a load.
    """
    match = SNAPSHOT_PATTERN.search(comment or "")
    return (match.group(1), match.group(2)) if match else None
