"""The validation material: the `Catalog` sheet and the unclassified examples."""

from collections.abc import Mapping, Sequence
from dataclasses import replace

import polars as pl
from googleapiclient.discovery import Resource

from pipelines.rj_crm__relatorio_engajamento_hsm.constants import (
    CATALOG_COLUMNS,
    CATALOG_DECISION,
    CATALOG_FILE,
    CATALOG_LEGEND,
    CATALOG_TAB,
    CONV_CONVERSATION,
    CONV_SESSION,
    DECISIONS,
    DRIVE_SHEET_MIME,
    EXAMPLES_FILE,
    EXAMPLES_SEPARATOR,
    EXAMPLES_TAB,
    EXCLUDE_COLOR,
    FLOW_COLOR,
    KEEP_COLOR,
    LEGEND_TAB,
    NEW_COLOR,
    SECRETARIAT_COLOR,
)
from pipelines.rj_crm__relatorio_engajamento_hsm.helpers.catalog import (
    Catalog,
    ValidCatalog,
    catalog_of,
    interpret_catalog,
)
from pipelines.rj_crm__relatorio_engajamento_hsm.helpers.drive import find_file, folder_link
from pipelines.rj_crm__relatorio_engajamento_hsm.helpers.requests import RequestError, skipped_status
from pipelines.rj_crm__relatorio_engajamento_hsm.helpers.sheets import (
    add_tab,
    create_sheet,
    format_sheet,
    read_sheet,
    replace_sheet,
)
from pipelines.rj_crm__relatorio_engajamento_hsm.types import Color, Conversation, Discovery, Prepared


def plan_validation(has_catalog: bool, conversations: int) -> str | None:
    """Return the skip status of a validation request, or nothing when it must run.

    An existing `Catalog` is never replaced: the requester renames it to ask again.
    """
    if has_catalog:
        return skipped_status(f"{CATALOG_FILE} já existe (renomeie o atual para gerar de novo)")

    if conversations == 0:
        return skipped_status("nenhuma conversa com resposta em texto do cidadão no período")

    return None


def build_catalog_frame(catalog: Catalog, examples: Mapping[str, Sequence[str]]) -> pl.DataFrame:
    """Return one row for each category, in discovery order, with the decision blank."""
    rows = [
        (name, description, EXAMPLES_SEPARATOR.join(examples.get(name, ())), "", "", "")
        for name, description in catalog.as_dict().items()
    ]

    return pl.DataFrame(rows, schema=list(CATALOG_COLUMNS), orient="row")


def build_examples_frame(conversations: Sequence[Conversation]) -> pl.DataFrame:
    """Return the session id and the conversation of every conversation of the period.

    It has no CPF and no phone.
    """
    return pl.DataFrame(
        {
            CONV_SESSION: [conversation.session_id for conversation in conversations],
            CONV_CONVERSATION: [conversation.text for conversation in conversations],
        }
    )


def read_validated_catalog(sheets: Resource, prepared: Prepared) -> ValidCatalog:
    """Read the `Catalog` of the campaign that the secretariat validated.

    Raise `RequestError` if there is none, or if no category has a decision yet, so
    the requester knows to ask for the validation material or to decide first.
    """
    if not prepared.catalog_id:
        raise RequestError("Catálogo não encontrado na pasta da campanha — peça primeiro o Material de validação")

    catalog = replace(interpret_catalog(read_sheet(sheets, prepared.catalog_id)), url=prepared.catalog_link)

    if catalog.empty:
        raise RequestError("Catálogo ainda não validado (nenhuma categoria com decisão Keep ou New)")

    return catalog


def publish_material(
    drive: Resource,
    sheets: Resource,
    folder_id: str,
    discovery: Discovery,
    conversations: Sequence[Conversation],
) -> str:
    """Publish the validation material in the folder of the campaign.

    The `Catalog` has a Keep/Exclude/New dropdown and a legend. It is only created, and
    the caller has checked that none exists. The examples sheet is replaced in place on
    each generation. Return the link of the folder.
    """
    flow_columns: dict[str, Color] = dict.fromkeys(CATALOG_COLUMNS[:3], FLOW_COLOR)
    secretariat_columns: dict[str, Color] = dict.fromkeys(CATALOG_COLUMNS[3:], SECRETARIAT_COLOR)
    created = create_sheet(
        drive,
        sheets,
        folder_id,
        (CATALOG_FILE, CATALOG_TAB),
        build_catalog_frame(catalog_of(discovery.catalog), discovery.examples),
    )
    format_sheet(
        sheets,
        created["id"],
        header_colors=flow_columns | secretariat_columns,
        dropdowns={CATALOG_DECISION: DECISIONS},
        highlights={
            CATALOG_DECISION: (
                (DECISIONS[0], KEEP_COLOR),
                (DECISIONS[1], EXCLUDE_COLOR),
                (DECISIONS[2], NEW_COLOR),
            )
        },
    )
    add_tab(sheets, created["id"], LEGEND_TAB, [list(row) for row in CATALOG_LEGEND])
    frame = build_examples_frame(conversations)
    existing = find_file(drive, EXAMPLES_FILE, folder_id, DRIVE_SHEET_MIME)

    if existing:
        replace_sheet(sheets, existing["id"], frame, EXAMPLES_TAB)
        examples_id = existing["id"]
    else:
        examples_id = create_sheet(drive, sheets, folder_id, (EXAMPLES_FILE, EXAMPLES_TAB), frame)["id"]

    format_sheet(sheets, examples_id, header_colors=dict.fromkeys(frame.columns, FLOW_COLOR))

    return folder_link(folder_id)
