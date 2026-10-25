"""Rules that the dispatch pipelines share: the query of the day, the switch of a campaign and the notices."""

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import date, datetime
from typing import Final

from toolkit.telemetry import logger

from .settings import NoticeSettings
from .utils import post_discord

ENVIRONMENTS: Final = ("staging", "production")
ACTIVE_VALUES: Final = frozenset({"1", "true", "yes", "sim", "ativo"})
SATURDAY: Final = 5
SUNDAY: Final = 6
WEEKEND: Final = (SATURDAY, SUNDAY)
DAYS_AHEAD_KEY: Final = "days_ahead_placeholder"
CONTROL_SHEET: Final = "https://docs.google.com/spreadsheets/d/1O-noD696ZjIr9X_Vl4ZKyFDyg0q9KHe9jacExdAp4ck/"
MENTIONS: Final = "<@821121576455634955> <@1458456241683824744> <@302518123066556426>"
"""The people that the notices of a stopped dispatch call."""

type Replacements = Mapping[str, str | int]
type Processor = Callable[[str, Replacements, date], str | None]


def skip_weekends(query: str, replacements: Replacements, today: date) -> str | None:
    """Fill the query, and skip the weekend. Return nothing on a Saturday or a Sunday.

    `days_ahead_placeholder` is how many days ahead the query looks. If that day is a weekend, the query
    looks two days further, so it lands on a Monday or a Tuesday. Raise `ValueError` if the placeholder is
    missing from the query or from `replacements`.
    """
    if f"{{{DAYS_AHEAD_KEY}}}" not in query:
        raise ValueError(f"The query must have the placeholder {{{DAYS_AHEAD_KEY}}}")

    if DAYS_AHEAD_KEY not in replacements:
        raise ValueError(f"The key {DAYS_AHEAD_KEY!r} must be in the replacements")

    weekday = today.weekday()

    if weekday in WEEKEND:
        logger.info("Weekend: the dispatch is skipped: weekday=%d", weekday)

        return None

    days_ahead = int(replacements[DAYS_AHEAD_KEY])

    if weekday + days_ahead in WEEKEND:
        days_ahead += 2

    logger.info("Query of the day: weekday=%d days_ahead=%d", weekday, days_ahead)

    return query.format_map({**replacements, DAYS_AHEAD_KEY: days_ahead})


PROCESSORS: Final[Mapping[str, Processor]] = {"skip_weekends": skip_weekends}


def format_query(query: str, replacements: Replacements, processor: str | None, today: date) -> str | None:
    """Fill the placeholders of a query, with a processor if one is named. A processor can give nothing to skip the run.

    A processor that does not exist is ignored with a warning.
    """
    if processor:
        function = PROCESSORS.get(processor)

        if function is not None:
            return function(query, replacements, today)

        logger.warning("Query processor not found, the query is used as it is: name=%s", processor)

    return query.format_map(replacements)


@dataclass(frozen=True)
class Switch:
    """The switch of a campaign: whether it may run, and the notice that tells why not."""

    active: bool
    notice: str | None = None


def is_active(raw: object) -> bool:
    """Tell whether the value of the column `ativo` is on: `1`, `true`, `yes`, `sim` or `ativo`, in any case."""
    return raw is not None and str(raw).strip().lower() in ACTIVE_VALUES


def read_expiration(raw: object) -> date | None:
    """Read the limit date of a campaign from its text, or nothing if it has none or cannot be read."""
    if raw is None:
        return None

    try:
        return datetime.strptime(str(raw)[:10], "%Y-%m-%d").date()  # noqa: DTZ007
    except ValueError:
        logger.warning("The limit date cannot be read, so there is no limit: value=%r", raw)

        return None


def decide_switch(row: Mapping[str, object] | None, campaign: str, environment: str, today: date) -> Switch:
    """Decide from the row of the control sheet whether a campaign may run today.

    There is no row, an environment that is not `staging` or `production`, no campaign name, a campaign that
    is off, and a limit date in the past: each one stops the run. The last two come with a notice for Discord.
    """
    if environment not in ENVIRONMENTS or not campaign:
        return Switch(False)

    if row is None:
        logger.info("No configuration for the campaign: campaign=%s environment=%s", campaign, environment)

        return Switch(False)

    if not is_active(row.get("ativo")):
        return Switch(
            False,
            f"{MENTIONS}\nPrefect flow run desativado em {CONTROL_SHEET}!\n"
            + f"📋 **Campanha:** {campaign}\n💻 **Ambiente:** {environment}\n"
            + "Desligue o scheduler no prefect ou mude o status para ativo para reativar o fluxo.",
        )

    expiration = read_expiration(row.get("data_limite_disparo"))

    if expiration is not None and expiration < today:
        return Switch(
            False,
            f"{MENTIONS}\nPrefect flow run atingiu a data limite em {CONTROL_SHEET}!\n"
            + f"📋 **Campanha:** {campaign}\n💻 **Ambiente:** {environment}\n"
            + f"📆 **Data limite do disparo:** {expiration}\nDesligue o scheduler no prefect ou altere a data limite.",
        )

    return Switch(True)


def no_destinations_notice(campaign: str, *, test_mode: bool, today: date) -> str:
    """Return the notice of a dispatch that found nobody to message."""
    prefix = "[TESTE] " if test_mode else ""
    title = f"⚠️ **{prefix}Disparo não realizado pois nenhum destinatário foi encontrado.**"

    return f"{title}\n📋 **Campanha:** {campaign}\n🕐 **Disparo realizado em:** {today}\n"


async def post_notice(message: str, *, errors: bool = False) -> None:
    """Post a message to the channel of the dispatches, or to the one of the errors.

    The channel is a setting. With no webhook, the notice is only logged.
    """
    settings = NoticeSettings()
    webhook = (settings.DISCORD_ERRORS_WEBHOOK_URL if errors else settings.DISCORD_WEBHOOK_URL).get_secret_value()

    if webhook:
        await post_discord(webhook, message)
    else:
        logger.warning("There is no Discord webhook for the notice, so it is only logged: %s", message)
