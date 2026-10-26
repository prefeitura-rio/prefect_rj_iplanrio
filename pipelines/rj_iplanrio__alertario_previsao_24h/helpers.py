"""Helpers of rj_iplanrio__alertario_previsao_24h."""

from collections import defaultdict
from collections.abc import Mapping, Sequence
from datetime import date, datetime
from hashlib import sha256

from defusedxml.ElementTree import fromstring
from toolkit.telemetry import logger

from pipelines.rj_iplanrio__alertario_previsao_24h.constants import (
    ALERT_CHANNEL,
    ALERT_TITLE,
    EXCERPT_LIMIT,
    INTENSE_PRECIPITATION,
    PERIODS_TABLE,
    SP_TZ,
    SYNOPTIC_TABLE,
    SYNOPTIC_TITLE,
    TIDE_FORMATS,
    TIDES_TABLE,
    ZONES_TABLE,
)
from pipelines.rj_iplanrio__alertario_previsao_24h.types import Forecast, PrecipitationAlert, Row


def localize(moment: datetime) -> datetime:
    """Return a time in São Paulo, which is the time itself if it has a zone."""
    return moment if moment.tzinfo else moment.replace(tzinfo=SP_TZ)


def read_number(text: str | None) -> float | None:
    """Return an attribute as a number, or nothing if it is empty."""
    return float(text) if text else None


def read_tide_time(text: str | None) -> str | None:
    """Return the time of a tide in ISO format, or nothing if it is empty or has an unknown format."""
    if not text:
        return None

    try:
        return localize(datetime.fromisoformat(text)).isoformat()
    except ValueError:
        pass

    for time_format in TIDE_FORMATS:
        try:
            return datetime.strptime(text, time_format).replace(tzinfo=SP_TZ).isoformat()
        except ValueError:
            continue

    logger.warning("Unknown time format of a tide: %s", text)

    return None


def build_tables(xml: str, execution_id: str, now: datetime) -> Forecast:
    """Read the forecast XML and return its rows by table, the synoptic text and the date of the XML.

    All the rows carry `execution_id`, so the tables join. `now` is the time of the run,
    and the date of the XML is `now` if the XML has none or a bad one. A number that is
    not a number raises `ValueError`, and a bad XML raises `ParseError`.
    """
    root = fromstring(xml)
    created = now

    if text := root.get("Createdate"):
        try:
            created = localize(datetime.fromisoformat(text))
        except ValueError:
            logger.warning("Bad Createdate in the XML, the run time replaces it: %s", text)
    else:
        logger.warning("No Createdate in the XML, the run time replaces it")

    common: Row = {
        "id_execucao": execution_id,
        "data_execucao": now.date().isoformat(),
        "hora_execucao": now.time().isoformat(),
    }
    tail: Row = {"data_alertario": created.isoformat(), "data_particao": now.date().isoformat()}
    synoptic_element = root.find("quadroSinotico")
    synoptic = (synoptic_element.get("sinotico") or synoptic_element.text or "") if synoptic_element is not None else ""
    tables: dict[str, list[Row]] = defaultdict(list)
    tables[SYNOPTIC_TABLE].append({**common, "sinotico": synoptic, **tail})

    for period in root.findall("previsao"):
        tables[PERIODS_TABLE].append(
            {
                **common,
                "data_periodo": period.get("datePeriodo"),
                "periodo": period.get("periodo"),
                "ceu": period.get("ceu"),
                "precipitacao": period.get("precipitacao"),
                "dir_vento": period.get("dirVento"),
                "vel_vento": period.get("velVento"),
                "temperatura": period.get("temperatura"),
                **tail,
            }
        )

    temperature = root.find("Temperatura")

    if temperature is not None:
        measured = temperature.get("data") or created.date().isoformat()

        for zone in temperature.findall("Zona"):
            tables[ZONES_TABLE].append(
                {
                    **common,
                    "zona": zone.get("zona"),
                    "temp_minima": read_number(zone.get("minima")),
                    "temp_maxima": read_number(zone.get("maxima")),
                    "data_temperatura": measured,
                    **tail,
                }
            )

    tides = root.find("TabuasMares")

    for tide in tides.findall("tabua") if tides is not None else []:
        tables[TIDES_TABLE].append(
            {
                **common,
                "data_hora": read_tide_time(tide.get("date")),
                "elevacao": tide.get("elevacao"),
                "altura": read_number(tide.get("altura")),
                **tail,
            }
        )

    return Forecast(execution_id=execution_id, tables=dict(tables), synoptic=synoptic, created=created)


def is_intense(text: str | None) -> bool:
    """Tell whether a forecast of rain is for moderate to heavy rain."""
    normalized = (text or "").strip().lower()

    return any(keyword in normalized for keyword in INTENSE_PRECIPITATION)


def find_alerts(periods: Sequence[Mapping[str, object]]) -> list[PrecipitationAlert]:
    """Return the periods of the forecast that have a forecast of heavy rain."""
    return [
        PrecipitationAlert(
            forecast_date=date.fromisoformat(str(period["data_periodo"])),
            periodo=str(period.get("periodo") or "").strip(),
            precipitacao=str(period["precipitacao"]).strip(),
        )
        for period in periods
        if is_intense(str(period.get("precipitacao") or ""))
    ]


def format_alert_message(
    alerts: Sequence[PrecipitationAlert], synoptic: str | None = None, synoptic_date: date | None = None
) -> str:
    """Return the Discord message of the alerts, grouped by day, with the synoptic text first if there is one.

    Raise `ValueError` if there is no alert.
    """
    if not alerts:
        raise ValueError("A message needs at least one alert")

    days: dict[date, list[PrecipitationAlert]] = defaultdict(list)

    for alert in alerts:
        days[alert.forecast_date].append(alert)

    lines = [ALERT_TITLE, ""]

    if synoptic and synoptic.strip():
        title = f"{SYNOPTIC_TITLE} \u2013 {synoptic_date:%d/%m/%Y}" if synoptic_date else SYNOPTIC_TITLE
        lines += [title, synoptic.strip(), ""]

    for day in sorted(days):
        lines.append(f"{day:%d/%m/%Y}")
        lines += [f"• {alert.periodo or '-'}: {alert.precipitacao}" for alert in days[day]]
        lines.append("")

    return "\n".join(lines).strip()


def hash_message(message: str) -> str:
    """Return the SHA-256 of a message, which names the alert in the log."""
    return sha256(message.encode()).hexdigest()


def decide_alert(
    stats: Mapping[str, int | None] | None, alert_hash: str, max_daily: int, min_interval_hours: int
) -> tuple[bool, str]:
    """Tell whether to send an alert, from the log of today, and why.

    The alert goes out unless today has the same alert, has `max_daily` alerts, or has
    an alert from less than `min_interval_hours` ago. `stats` is nothing without a log.
    """
    total = (stats or {}).get("total_alerts_today") or 0

    if stats is None or total == 0:
        return True, "No alert sent today"

    if stats.get("hash_count_today"):
        return False, f"Duplicate alert, the hash {alert_hash[:8]} was sent today"

    if total >= max_daily:
        return False, f"Daily limit reached: {total} of {max_daily}"

    hours = stats.get("hours_since_last_alert")

    if hours is not None and hours < min_interval_hours:
        return False, f"Too soon: {hours} hours since the last alert, the minimum is {min_interval_hours}"

    return True, f"All checks passed: alert {total + 1} of {max_daily} today"


def build_log_rows(
    alerts: Sequence[PrecipitationAlert], *, execution_id: str, message: str, message_id: str | None, sent_at: datetime
) -> list[Row]:
    """Return the rows of the alert log, one for each alert of a sent message."""
    alert_date = sent_at.date().isoformat()

    return [
        {
            "alert_date": alert_date,
            "id_execucao": execution_id,
            "forecast_date": alert.forecast_date.isoformat(),
            "periodo": alert.periodo,
            "precipitacao": alert.precipitacao,
            "alert_hash": hash_message(message),
            "severity_level": "info",
            "sent_at": sent_at.isoformat(),
            "discord_message_id": message_id,
            "webhook_channel": ALERT_CHANNEL,
            "message_excerpt": message[:EXCERPT_LIMIT],
            "data_particao": alert_date,
        }
        for alert in alerts
    ]
