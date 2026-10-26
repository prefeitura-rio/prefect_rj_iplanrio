"""Helpers of rj_cor__meteorologia_redemet."""

from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime, timedelta

from pydantic import JsonValue

from pipelines.rj_cor__meteorologia_redemet.constants import SP_TZ, TIME_FORMAT


def read_dates(first_date: str | None, last_date: str | None) -> tuple[date, date, bool]:
    """Return the first and last date to fetch, in UTC, and whether it is a backfill.

    Without a first date the range is yesterday to today, and it is not a backfill.
    """
    if first_date:
        return date.fromisoformat(first_date), date.fromisoformat(last_date or first_date), True

    today = datetime.now(UTC).date()

    return today - timedelta(days=1), today, False


def list_stamps(first_date: date, last_date: date) -> list[str]:
    """Return every hour from the first to the last date, as `YYYYMMDDHH`.

    ```python
    list_stamps(date(2026, 1, 31), date(2026, 2, 1))[22:26]
    # ["2026013122", "2026013123", "2026020100", "2026020101"]
    ```
    """
    days = (last_date - first_date).days + 1

    return [f"{first_date + timedelta(days=day):%Y%m%d}{hour:02}" for day in range(days) for hour in range(24)]


def read_observation(body: JsonValue) -> dict[str, JsonValue] | None:
    """Return the observation of a response, or nothing if the API has none."""
    data = body.get("data") if isinstance(body, dict) else None

    if not isinstance(body, dict) or body.get("status") is not True or not isinstance(data, dict) or "data" not in data:
        return None

    return data


def read_temperature(text: str) -> int | None:
    """Return the temperature of `25ºC` as 25, or nothing for `NILºC`."""
    number = text[:-2]

    return None if number == "NIL" else int(number)


def read_humidity(text: str) -> int | None:
    """Return the humidity of `70%` as 70, or nothing without a percent sign."""
    return int(text[:-1]) if "%" in text else None


def build_rows(
    observations: Sequence[Mapping[str, JsonValue]], backfill: bool, today: date
) -> list[dict[str, JsonValue]]:
    """Return the measurements, one for each station and time, in the time of São Paulo.

    A repeated station and time keeps the first. Without a backfill only the
    measurements of `today` stay. The sky text starts with a capital. `ano`, `mes` and
    `dia` are the day of the measurement as text, like the legacy table.
    """
    rows: dict[tuple[str, str], dict[str, JsonValue]] = {}

    for observation in observations:
        measured = datetime.strptime(str(observation["data"]), TIME_FORMAT).replace(tzinfo=UTC).astimezone(SP_TZ)

        if not backfill and measured.date() != today:
            continue

        station = str(observation["localidade"])
        key = (station, measured.isoformat())

        if key in rows:
            continue

        sky = observation.get("ceu")
        rows[key] = {
            "id_estacao": station,
            "data_medicao": measured.strftime("%Y-%m-%d %H:%M:%S"),
            "temperatura": read_temperature(str(observation["temperatura"])),
            "umidade": read_humidity(str(observation["ur"])),
            "condicoes_tempo": observation.get("condicoes_tempo"),
            "ceu": sky.capitalize() if isinstance(sky, str) else sky,
            "teto": observation.get("teto"),
            "visibilidade": observation.get("visibilidade"),
            "ano": f"{measured:%Y}",
            "mes": f"{measured:%m}",
            "dia": f"{measured:%d}",
        }

    return list(rows.values())
