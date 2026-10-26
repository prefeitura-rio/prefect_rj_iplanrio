"""Helpers of rj_smas__api_datametrica_agendamentos."""

from collections.abc import Sequence
from datetime import date, datetime, timedelta
from json import loads

from pydantic import JsonValue

from pipelines.rj_smas__api_datametrica_agendamentos.constants import LONG_AHEAD, SP_TZ, USER_AGENT, WEEKEND
from utils.utils import render_query

FIELDS = {
    "id": "id",
    "id_capacidade": "id_capacidade",
    "nome_completo": "nome_completo",
    "primeiro_nome": "primeiro_nome",
    "cpf": "cpf",
    "telefone": "telefone",
    "tipo": "tipo",
    "data_hora": "data_hora",
    "nome": "unidade_nome",
    "endereco": "unidade_endereco",
    "bairro": "unidade_bairro",
}
"""The fields of the API that the table keeps, with their names in the table."""


def read_target_date(today: date, given: str | None) -> date | None:
    """Return the date to fetch: the one given, or two days ahead.

    It is four days ahead on Thursday and Friday.

    On Saturday and Sunday there is nothing to fetch, and the result is nothing.
    """
    if given:
        return date.fromisoformat(given)

    if today.weekday() in WEEKEND:
        return None

    return today + timedelta(days=4 if today.weekday() in LONG_AHEAD else 2)


def today_in_sao_paulo() -> date:
    """Return today in São Paulo."""
    return datetime.now(SP_TZ).date()


def build_headers(token: str, proxy_token: str) -> dict[str, str]:
    """Return the headers of a request through the proxy."""
    return {
        "X-Proxy-Api-Token": proxy_token,
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "User-Agent": USER_AGENT,
        "Accept": "application/json",
    }


def build_rows(records: Sequence[JsonValue]) -> list[dict[str, JsonValue]]:
    """Return the appointments as rows with the fields of the table.

    A record may come as JSON text, and an empty text is skipped.
    Raise `KeyError` for a record that lacks a field.
    """
    rows: list[dict[str, JsonValue]] = []

    for record in records:
        data = loads(record) if isinstance(record, str) and record.strip() else record

        if not isinstance(data, dict):
            continue

        rows.append({column: data[field] for field, column in FIELDS.items()})

    return rows


def build_dispatch_parameters(table: str, days_ahead: int) -> dict[str, JsonValue]:
    """Return the parameters of the dispatch flow of the Cadastro Único reminders.

    `table` is the table of the appointments that the dispatch query reads.
    """
    hsm = "confirma_agendamento_cadunico_prod_v2"
    query = render_query(
        __file__,
        "cadunico_dispatch",
        table=table,
        days_ahead=days_ahead,
        hsm_name=hsm,
        dispatched_days=1,
        legacy_hsm_id=101,
    )

    return {
        "campaign_name": hsm,
        "query": query,
        "data_extension_filename": "whatsapp_cadunico_",
        "de_columns": ["nome_sobrenome", "unidade_cras", "data", "hora", "endereco"],
        "dataset_id": "brutos_salesforce",
        "table_id": "disparos_efetuados",
        "dump_mode": "append",
        "test_mode": False,
        "sleep_minutes": 1,
        "flow_environment": "production",
        "filter_dispatched_phones_or_cpfs": None,
        "filter_duplicated_phones": False,
        "filter_duplicated_cpfs": True,
        "whitelist_percentage": 0,
    }
