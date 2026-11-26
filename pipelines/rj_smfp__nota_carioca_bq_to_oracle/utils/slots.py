"""Tabelas físicas A e B atrás de sinônimos: a carga vai para a inativa e a troca é só repontar os sinônimos."""

import re
from dataclasses import dataclass
from datetime import UTC, datetime

import oracledb

from pipelines.rj_smfp__nota_carioca_bq_to_oracle.utils.oracle import (
    MANAGED_TABLE_MARKER,
    QUERIES_ANCHOR,
    OracleConfig,
    assert_managed_existing_table,
    assert_managed_table,
    assert_privileges,
    connect,
    fetch_rows,
    validate_identifier,
)
from prefect_rj_iplanrio.sql import load_query

SLOTS = ("A", "B")
CONSUMER_SYNONYM_OWNERS = ("NFSE_SIGA", "NFSE_USER", "NFSE_OWNER")
CONSUMER_GRANTS = (
    ("RL_NFSE", "SELECT"),
    ("RL_NFSE_SIGA", "SELECT"),
    ("RL_NFSEOWNER_DRL", "SELECT"),
    ("NFSE_OWNER", "SELECT, ALTER, DELETE"),
)
# Tabela particionada aparece em all_objects também como TABLE PARTITION/SUBPARTITION, com o nome da tabela.
REPLACEABLE_OBJECT_TYPES = {"TABLE", "TABLE PARTITION", "TABLE SUBPARTITION", "SYNONYM"}
SNAPSHOT_PATTERN = re.compile(r"snapshot do BigQuery de ([0-9-]+ [0-9:]+) UTC; carregada em ([0-9-]+ [0-9:]+) UTC")
TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"
# Privilégios de objeto que um consumidor pode receber na tabela física.
GRANTABLE_PRIVILEGES = frozenset({"SELECT", "INSERT", "UPDATE", "DELETE", "ALTER"})


@dataclass(frozen=True)
class OracleTarget:
    """Onde a carga grava e quem a consome: a pasta do segredo, o schema de destino e os consumidores.

    Prod, homologação e desenvolvimento usam os consumidores padrão; o homteste, no mesmo banco da homologação, tem
    os seus (``NFSE_SIGA_TESTE``, ``RL_NFSE_TESTE``...), para a troca de um ambiente não repontar os sinônimos do outro.

    :param infisical_secret_path: Pasta do segredo do Oracle no Infisical.
    :param schema: Schema das tabelas físicas; ``None`` usa o ``DB_SCHEMA`` do segredo.
    :param synonym_owners: Donos dos sinônimos dos consumidores, além do próprio schema.
    :param grants: Quem recebe acesso à tabela física, e com quais privilégios (``SELECT, ALTER, DELETE``).
    :raises ValueError: Se algum nome não for um identificador válido ou algum privilégio não for permitido.
    """

    infisical_secret_path: str
    schema: str | None = None
    synonym_owners: tuple[str, ...] = CONSUMER_SYNONYM_OWNERS
    grants: tuple[tuple[str, str], ...] = CONSUMER_GRANTS

    def __post_init__(self) -> None:
        """Normaliza os nomes para maiúsculas e confere nomes e privilégios."""
        if self.schema is not None:
            object.__setattr__(self, "schema", validate_identifier(self.schema))
        object.__setattr__(self, "synonym_owners", tuple(validate_identifier(owner) for owner in self.synonym_owners))
        object.__setattr__(
            self,
            "grants",
            tuple(
                (validate_identifier(grantee), validate_privileges(privileges)) for grantee, privileges in self.grants
            ),
        )


def validate_privileges(privileges: str) -> str:
    """Normaliza e confere a lista de privilégios de um grant.

    :param privileges: Privilégios separados por vírgula, como ``SELECT, ALTER, DELETE``.
    :returns: Os privilégios em maiúsculas, separados por ``, ``.
    :raises ValueError: Se a lista estiver vazia ou tiver um privilégio fora de ``GRANTABLE_PRIVILEGES``.
    """
    items = [item.strip().upper() for item in privileges.split(",") if item.strip()]
    invalid = sorted(set(items) - GRANTABLE_PRIVILEGES)
    if not items or invalid:
        raise ValueError(
            f"Privilégios inválidos para o grant: {privileges!r} (permitidos: {sorted(GRANTABLE_PRIVILEGES)})."
        )
    return ", ".join(items)


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


def synonym_owners(schema: str, consumers: tuple[str, ...] = CONSUMER_SYNONYM_OWNERS) -> tuple[str, ...]:
    """Lista os donos dos sinônimos da tabela, começando pelo schema de destino.

    :param schema: Schema das tabelas físicas.
    :param consumers: Donos dos sinônimos dos consumidores.
    :returns: Schema de destino e schemas dos consumidores.
    """
    return (schema, *consumers)


def choose_slots(
    base: str, schema: str, synonyms: list[SynonymTarget], consumers: tuple[str, ...] = CONSUMER_SYNONYM_OWNERS
) -> SlotPlan:
    """Define a tabela em uso e a que recebe a carga a partir dos sinônimos.

    A tabela em uso é a apontada pelo sinônimo do schema de destino ou, se ele
    ainda não existir (troca interrompida), pela primeira tabela física apontada
    por um sinônimo dos consumidores.

    :param base: Nome estável da tabela.
    :param schema: Schema das tabelas físicas.
    :param synonyms: Sinônimos com o nome ``base``, de qualquer dono.
    :param consumers: Donos dos sinônimos dos consumidores.
    :returns: Tabela em uso e tabela que recebe a carga.
    :raises PermissionError: Se algum sinônimo da pipeline apontar para um objeto
        que ela não gerencia.
    """
    slot_tables = {slot_table_name(base, slot): slot for slot in SLOTS}
    owners = synonym_owners(schema, consumers)
    ours = [synonym for synonym in synonyms if synonym.owner in owners]
    for synonym in ours:
        if synonym.table_owner != schema or (synonym.table_name not in slot_tables and synonym.table_name != base):
            raise PermissionError(
                f"O sinônimo {synonym.owner}.{base} aponta para {synonym.table_owner}.{synonym.table_name}, "
                "que a pipeline não gerencia. Nada foi alterado."
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


def read_synonyms(
    cursor: oracledb.Cursor, schema: str, base: str, consumers: tuple[str, ...] = CONSUMER_SYNONYM_OWNERS
) -> list[SynonymTarget]:
    """Lê os sinônimos da pipeline com o nome estável da tabela.

    :param cursor: Cursor de uma conexão aberta.
    :param schema: Schema das tabelas físicas.
    :param base: Nome estável da tabela.
    :param consumers: Donos dos sinônimos dos consumidores.
    :returns: Sinônimos do schema de destino e dos consumidores.
    """
    owners = synonym_owners(schema, consumers)
    return [
        SynonymTarget(owner=str(row["owner"]), table_owner=str(row["table_owner"]), table_name=str(row["table_name"]))
        for row in fetch_rows(cursor, "get_synonyms", {"synonym_name": base})
        if row["owner"] in owners
    ]


def create_synonym(cursor: oracledb.Cursor, owner: str, base: str, schema: str, table: str) -> None:
    """Cria ou repõe um sinônimo apontando para uma tabela física.

    :param cursor: Cursor de uma conexão aberta.
    :param owner: Dono do sinônimo.
    :param base: Nome do sinônimo.
    :param schema: Schema da tabela física.
    :param table: Tabela física.
    """
    cursor.execute(load_query(QUERIES_ANCHOR, "create_synonym", owner=owner, synonym=base, schema=schema, table=table))


def assert_replaceable_legacy_table(cursor: oracledb.Cursor, schema: str, base: str) -> bool:
    """Confere se a tabela única antiga, se existir, pode dar lugar ao sinônimo.

    Roda antes de qualquer alteração, para que uma tabela que a pipeline não
    criou com o nome ``BQLOAD_<tabela>`` interrompa o run sem mexer em nada.

    :param cursor: Cursor de uma conexão aberta.
    :param schema: Schema das tabelas físicas.
    :param base: Nome estável da tabela.
    :returns: Se a tabela única antiga existe.
    :raises PermissionError: Se a tabela existir sem a marca da pipeline, ou se
        houver com esse nome outro tipo de objeto, que impediria criar o sinônimo.
    """
    types = {
        str(row["object_type"])
        for row in fetch_rows(cursor, "get_objects_named", {"owner": schema, "object_name": base})
    }
    others = sorted(types - REPLACEABLE_OBJECT_TYPES)
    if others:
        raise PermissionError(
            f"Existe {others} com o nome {schema}.{base}, que impede criar o sinônimo. Nada foi alterado."
        )
    rows = fetch_rows(cursor, "get_table_comment", {"owner": schema, "table_name": base})
    if not rows:
        return False
    comment = rows[0]["comments"]
    assert_managed_table(base, None if comment is None else str(comment))
    return True


def resolve_slots(
    config: OracleConfig, base: str, consumers: tuple[str, ...] = CONSUMER_SYNONYM_OWNERS
) -> tuple[SlotPlan, list[str]]:
    """Descobre a tabela em uso e a que recebe a carga, corrigindo trocas interrompidas.

    :param config: Configuração da conexão.
    :param base: Nome estável da tabela.
    :param consumers: Donos dos sinônimos dos consumidores.
    :returns: Plano das tabelas e donos dos sinônimos repontados para a tabela em
        uso.
    :raises PermissionError: Se faltar privilégio, se algum sinônimo apontar para
        um objeto que a pipeline não gerencia ou se existir uma tabela
        ``BQLOAD_<tabela>`` que a pipeline não criou.
    """
    with connect(config) as connection, connection.cursor() as cursor:
        assert_privileges(cursor, config.schema)
        assert_replaceable_legacy_table(cursor, config.schema, base)
        synonyms = read_synonyms(cursor, config.schema, base, consumers)
        plan = choose_slots(base, config.schema, synonyms, consumers)
        realigned = synonyms_to_realign(plan, synonyms)
        for owner in realigned:
            create_synonym(cursor, owner, base, config.schema, str(plan.active))
    return plan, realigned


def grant_access(config: OracleConfig, table: str, grants: tuple[tuple[str, str], ...] = CONSUMER_GRANTS) -> None:
    """Concede aos consumidores o acesso à tabela física.

    Roda a cada carga, antes da troca: recriar a tabela apaga os grants. Os
    comandos podem ser repetidos sem efeito colateral.

    :param config: Configuração da conexão.
    :param table: Tabela física.
    :param grants: Quem recebe o acesso, e com quais privilégios.
    :raises PermissionError: Se a tabela não existir ou não pertencer à pipeline.
    """
    with connect(config) as connection, connection.cursor() as cursor:
        assert_managed_existing_table(cursor, config.schema, table)
        for grantee, privileges in grants:
            cursor.execute(
                load_query(
                    QUERIES_ANCHOR,
                    "grant_on_table",
                    privileges=privileges,
                    schema=config.schema,
                    table=table,
                    grantee=grantee,
                )
            )


def load_comment(source: str, snapshot_modified: datetime, loaded_at: datetime) -> str:
    """Monta o comentário da tabela física após uma carga completa.

    :param source: Tabela de origem no BigQuery.
    :param snapshot_modified: Última alteração do BigQuery na foto carregada.
    :param loaded_at: Fim da carga.
    :returns: Comentário com a marca da pipeline, a origem, a foto e o horário.
    """
    return (
        f"{MANAGED_TABLE_MARKER}: carga a partir de {source}; "
        f"snapshot do BigQuery de {snapshot_modified.astimezone(UTC):{TIMESTAMP_FORMAT}} UTC; "
        f"carregada em {loaded_at.astimezone(UTC):{TIMESTAMP_FORMAT}} UTC"
    )


def parse_load_comment(comment: str | None) -> tuple[str, str] | None:
    """Extrai do comentário a foto do BigQuery carregada e o horário da carga.

    :param comment: Comentário da tabela física.
    :returns: ``(foto do BigQuery, horário da carga)`` em UTC, ou ``None`` se a
        tabela ainda não terminou uma carga.
    """
    match = SNAPSHOT_PATTERN.search(comment or "")
    return (match.group(1), match.group(2)) if match else None


def record_load(config: OracleConfig, table: str, source: str, snapshot_modified: datetime) -> str:
    """Grava no comentário da tabela física qual foto do BigQuery ela contém.

    :param config: Configuração da conexão.
    :param table: Tabela física.
    :param source: Tabela de origem no BigQuery.
    :param snapshot_modified: Última alteração do BigQuery na foto carregada.
    :returns: Comentário gravado.
    :raises PermissionError: Se a tabela não existir ou não pertencer à pipeline.
    """
    comment = load_comment(source, snapshot_modified, datetime.now(UTC))
    with connect(config) as connection, connection.cursor() as cursor:
        assert_managed_existing_table(cursor, config.schema, table)
        cursor.execute(
            load_query(
                QUERIES_ANCHOR,
                "comment_on_table",
                schema=config.schema,
                table=table,
                comment=comment.replace("'", "''"),
            )
        )
    return comment


def swap_synonyms(
    config: OracleConfig, plan: SlotPlan, consumers: tuple[str, ...] = CONSUMER_SYNONYM_OWNERS
) -> list[str]:
    """Aponta os sinônimos para a tabela recém-carregada.

    Os sinônimos dos consumidores são trocados primeiro e o do schema de destino
    por último, porque é ele que define a tabela em uso no próximo run. Na
    primeira troca, a antiga tabela única ``BQLOAD_<tabela>`` é apagada para dar
    lugar ao sinônimo de mesmo nome; só nesse instante quem lê
    ``<schema>.BQLOAD_<tabela>`` direto fica sem o objeto, por milissegundos.

    :param config: Configuração da conexão.
    :param plan: Tabela em uso e tabela recém-carregada.
    :param consumers: Donos dos sinônimos dos consumidores.
    :returns: Ações executadas, em texto, para o log.
    :raises PermissionError: Se a tabela carregada ou a tabela única antiga não
        pertencerem à pipeline; a conferência acontece antes de qualquer
        sinônimo ser repontado.
    """
    actions = []
    with connect(config) as connection, connection.cursor() as cursor:
        assert_managed_existing_table(cursor, config.schema, plan.inactive)
        legacy = assert_replaceable_legacy_table(cursor, config.schema, plan.base)
        for owner in consumers:
            create_synonym(cursor, owner, plan.base, config.schema, plan.inactive)
            actions.append(f"{owner}.{plan.base} → {plan.inactive}")
        if legacy:
            cursor.execute(load_query(QUERIES_ANCHOR, "drop_table", schema=config.schema, table=plan.base))
            actions.append(f"tabela única antiga {config.schema}.{plan.base} apagada")
        create_synonym(cursor, config.schema, plan.base, config.schema, plan.inactive)
        actions.append(f"{config.schema}.{plan.base} → {plan.inactive}")
    return actions


def refresh_synonyms(
    config: OracleConfig,
    base: str,
    consumers: tuple[str, ...] = CONSUMER_SYNONYM_OWNERS,
    grants: tuple[tuple[str, str], ...] = CONSUMER_GRANTS,
) -> list[str]:
    """Reaponta todos os sinônimos para a tabela física já em uso, sem carga.

    Serve para criar sinônimos de um dono novo ou corrigir um sinônimo removido
    sem esperar uma carga completa. Os grants dos consumidores são reaplicados na
    tabela em uso antes, para o sinônimo novo já funcionar.

    :param config: Configuração da conexão.
    :param base: Nome estável da tabela.
    :param consumers: Donos dos sinônimos dos consumidores.
    :param grants: Quem recebe o acesso à tabela em uso, e com quais privilégios.
    :returns: Ações executadas, em texto, para o log.
    :raises LookupError: Se ainda não houver tabela A/B em uso.
    :raises PermissionError: Se faltar privilégio, se algum sinônimo apontar para
        um objeto que a pipeline não gerencia ou se a migração para A/B não tiver
        terminado (a tabela única antiga ainda existe).
    """
    with connect(config) as connection, connection.cursor() as cursor:
        assert_privileges(cursor, config.schema)
        if assert_replaceable_legacy_table(cursor, config.schema, base):
            raise PermissionError(
                f"{config.schema}.{base} ainda é a tabela única antiga; rode a carga completa (mode=full) "
                "para terminar a migração para A/B. Nada foi alterado."
            )
        plan = choose_slots(base, config.schema, read_synonyms(cursor, config.schema, base, consumers), consumers)
        if plan.active is None:
            raise LookupError(f"{base}: nenhuma tabela A/B em uso; rode a carga completa (mode=full).")
        assert_managed_existing_table(cursor, config.schema, plan.active)
    grant_access(config, plan.active, grants)
    actions = []
    with connect(config) as connection, connection.cursor() as cursor:
        for owner in (*consumers, config.schema):
            create_synonym(cursor, owner, base, config.schema, plan.active)
            actions.append(f"{owner}.{base} → {plan.active}")
    return actions
