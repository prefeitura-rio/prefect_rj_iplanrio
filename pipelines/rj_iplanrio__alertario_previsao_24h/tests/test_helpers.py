from datetime import date, datetime

import pytest
from defusedxml.ElementTree import ParseError

from pipelines.rj_iplanrio__alertario_previsao_24h.constants import SP_TZ
from pipelines.rj_iplanrio__alertario_previsao_24h.helpers import (
    build_log_rows,
    build_tables,
    decide_alert,
    find_alerts,
    format_alert_message,
    hash_message,
    read_tide_time,
)
from pipelines.rj_iplanrio__alertario_previsao_24h.types import PrecipitationAlert

NOW = datetime(2026, 7, 17, 12, 0, tzinfo=SP_TZ)

XML = """<Previsao Createdate="2026-07-17T10:30:00">
  <previsao datePeriodo="2026-07-17" periodo="Manhã" ceu="Nublado" precipitacao="Pancadas de chuva"
            dirVento="S" velVento="Fraco" temperatura="Estável"/>
  <previsao datePeriodo="2026-07-17" periodo="Tarde" ceu="Claro" precipitacao="Sem chuva"
            dirVento="N" velVento="Fraco" temperatura="Em declínio"/>
  <quadroSinotico sinotico="Ventos úmidos."/>
  <Temperatura data="2026-07-17">
    <Zona zona="Norte" maxima="30.5" minima=""/>
  </Temperatura>
  <TabuasMares>
    <tabua elevacao="Preamar" date="17/07/2026 04:10:00" altura="1.2"/>
    <tabua elevacao="Baixa-mar" date="not a time" altura=""/>
  </TabuasMares>
</Previsao>"""


def at(day: int, hour: int) -> datetime:
    """Return a time of July 2026 in São Paulo."""
    return datetime(2026, 7, day, hour, tzinfo=SP_TZ)


def alerts_of(*items: tuple[date, str, str]) -> list[PrecipitationAlert]:
    """Build alerts from tuples of day, period and forecast."""
    return [PrecipitationAlert(*item) for item in items]


def test_the_xml_gives_four_tables_that_share_the_execution_id() -> None:
    """Read the periods, the zones and the tides, with the synoptic text and the date of the XML."""
    forecast = build_tables(XML, "run-1", NOW)
    tables = forecast["tables"]

    assert sorted(tables) == ["dim_mares", "dim_previsao_periodo", "dim_quadro_sinotico", "dim_temperatura_zona"]
    assert all(row["id_execucao"] == "run-1" for rows in tables.values() for row in rows)
    assert forecast["synoptic"] == "Ventos úmidos."
    assert forecast["created"] == datetime(2026, 7, 17, 10, 30, tzinfo=SP_TZ)
    assert [row["periodo"] for row in tables["dim_previsao_periodo"]] == ["Manhã", "Tarde"]
    assert tables["dim_previsao_periodo"][0]["data_particao"] == "2026-07-17"
    assert (tables["dim_temperatura_zona"][0]["temp_maxima"], tables["dim_temperatura_zona"][0]["temp_minima"]) == (
        30.5,
        None,
    )
    assert [(row["data_hora"], row["altura"]) for row in tables["dim_mares"]] == [
        ("2026-07-17T04:10:00-03:00", 1.2),
        (None, None),
    ]


@pytest.mark.parametrize(
    ("xml", "created"),
    [
        pytest.param("<Previsao/>", NOW, id="no_date_in_the_xml"),
        pytest.param('<Previsao Createdate="yesterday"/>', NOW, id="a_bad_date_in_the_xml"),
        pytest.param('<Previsao Createdate="2026-07-17T09:00:00-03:00"/>', at(17, 9), id="a_date_with_a_zone"),
    ],
)
def test_the_date_of_the_xml_falls_back_to_the_run_time(xml: str, created: datetime) -> None:
    """Use the run time when the XML has no usable date."""
    assert build_tables(xml, "run-1", NOW)["created"] == created


def test_an_xml_without_periods_zones_or_tides_still_has_the_synoptic_row() -> None:
    """Return one synoptic row with an empty text."""
    tables = build_tables("<Previsao/>", "run-1", NOW)["tables"]

    assert list(tables) == ["dim_quadro_sinotico"]
    assert tables["dim_quadro_sinotico"][0]["sinotico"] == ""


def test_a_malformed_xml_raises() -> None:
    """Fail the run, so no empty table is loaded."""
    with pytest.raises(ParseError):
        build_tables("<Previsao>", "run-1", NOW)


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        pytest.param("2026-07-17T04:10:00", "2026-07-17T04:10:00-03:00", id="iso"),
        pytest.param("2026-07-17 04:10:00", "2026-07-17T04:10:00-03:00", id="iso_with_a_space"),
        pytest.param("17/07/2026 04:10:00", "2026-07-17T04:10:00-03:00", id="brazilian"),
        pytest.param("04h10", None, id="unknown"),
        pytest.param("", None, id="empty"),
        pytest.param(None, None, id="missing"),
    ],
)
def test_the_time_of_a_tide_is_read_in_three_formats(text: str | None, expected: str | None) -> None:
    """Return the ISO time in São Paulo, or nothing."""
    assert read_tide_time(text) == expected


def test_only_the_periods_with_moderate_to_heavy_rain_are_alerts() -> None:
    """Keep the matching periods, in any case of letters."""
    periods = [
        {"data_periodo": "2025-01-01", "periodo": "Manhã", "precipitacao": "Sem chuva"},
        {"data_periodo": "2025-01-01", "periodo": "Tarde", "precipitacao": "Chuva moderada a forte isolada"},
        {"data_periodo": "2025-01-02", "periodo": "Manhã", "precipitacao": "Chuva fraca a moderada"},
        {"data_periodo": "2025-01-03", "periodo": " Noite ", "precipitacao": "PANCADAS DE CHUVA"},
        {"data_periodo": "2025-01-04", "periodo": "Manhã", "precipitacao": None},
    ]

    assert find_alerts(periods) == alerts_of(
        (date(2025, 1, 1), "Tarde", "Chuva moderada a forte isolada"),
        (date(2025, 1, 3), "Noite", "PANCADAS DE CHUVA"),
    )


def test_the_message_groups_the_alerts_by_day_after_the_synoptic_text() -> None:
    """Match the layout that the channel expects."""
    alerts = alerts_of(
        (date(2025, 11, 28), "Manhã", "Pancadas de chuva"),
        (date(2025, 11, 27), "Manhã", "Pancadas de chuva isoladas"),
        (date(2025, 11, 27), "Tarde", "Pancadas de chuva"),
    )

    assert format_alert_message(alerts, "Chuva.", date(2025, 11, 27)) == (
        "⚠️ Previsão de chuva – próximos dias (AlertaRio)\n\n"
        "Quadro sinótico – 27/11/2025\nChuva.\n\n"
        "27/11/2025\n• Manhã: Pancadas de chuva isoladas\n• Tarde: Pancadas de chuva\n\n"
        "28/11/2025\n• Manhã: Pancadas de chuva"
    )


@pytest.mark.parametrize(
    ("synoptic", "synoptic_date", "title"),
    [
        pytest.param(None, None, None, id="no_synoptic"),
        pytest.param("  ", date(2025, 11, 27), None, id="a_blank_synoptic"),
        pytest.param("Chuva.", None, "Quadro sinótico", id="a_synoptic_without_date"),
    ],
)
def test_the_synoptic_title_follows_what_is_known(
    synoptic: str | None, synoptic_date: date | None, title: str | None
) -> None:
    """Skip the synoptic block, or leave the date out of its title."""
    message = format_alert_message(alerts_of((date(2025, 11, 27), "", "Pancadas de chuva")), synoptic, synoptic_date)

    assert ("Quadro sinótico" in message) is (title is not None)
    assert title is None or title + "\n" in message
    assert "• -: Pancadas de chuva" in message


def test_a_message_without_alerts_is_refused() -> None:
    """Raise, so an empty alert never goes out."""
    with pytest.raises(ValueError, match="alert"):
        format_alert_message([])


@pytest.mark.parametrize(
    ("stats", "send", "reason"),
    [
        pytest.param(None, True, "No alert", id="no_log"),
        pytest.param(
            {"total_alerts_today": 0, "hash_count_today": 0, "hours_since_last_alert": None},
            True,
            "No alert",
            id="no_alert_today",
        ),
        pytest.param(
            {"total_alerts_today": 1, "hash_count_today": 1, "hours_since_last_alert": 9},
            False,
            "Duplicate",
            id="same_alert",
        ),
        pytest.param(
            {"total_alerts_today": 2, "hash_count_today": 0, "hours_since_last_alert": 9},
            False,
            "Daily limit",
            id="daily_limit",
        ),
        pytest.param(
            {"total_alerts_today": 1, "hash_count_today": 0, "hours_since_last_alert": 3},
            False,
            "Too soon",
            id="too_soon",
        ),
        pytest.param(
            {"total_alerts_today": 1, "hash_count_today": 0, "hours_since_last_alert": 4},
            True,
            "All checks",
            id="interval_met",
        ),
        pytest.param(
            {"total_alerts_today": 1, "hash_count_today": 0, "hours_since_last_alert": None},
            True,
            "All checks",
            id="no_time_known",
        ),
    ],
)
def test_the_alert_goes_out_unless_a_rule_of_the_day_stops_it(
    stats: dict[str, int | None] | None, send: bool, reason: str
) -> None:
    """Apply the duplicate, daily and interval rules in that order, with a limit of 2 and 4 hours."""
    decided, why = decide_alert(stats, "abcdef123456", 2, 4)

    assert decided is send
    assert reason in why


def test_the_log_has_one_row_for_each_alert_of_the_message() -> None:
    """Keep the dates, the hash, the message id and an excerpt that fits the limit."""
    alerts = alerts_of(
        (date(2025, 12, 3), "Manhã", "Pancadas de chuva"), (date(2025, 12, 4), "Tarde", "Pancadas de chuva")
    )
    sent_at = datetime(2025, 12, 3, 12, 0, tzinfo=SP_TZ)

    rows = build_log_rows(alerts, execution_id="abc", message="m" * 600, message_id="disc-1", sent_at=sent_at)

    assert [row["forecast_date"] for row in rows] == ["2025-12-03", "2025-12-04"]
    assert all(row["alert_hash"] == hash_message("m" * 600) for row in rows)
    assert (rows[0]["alert_date"], rows[0]["data_particao"], rows[0]["sent_at"]) == (
        "2025-12-03",
        "2025-12-03",
        "2025-12-03T12:00:00-03:00",
    )
    assert (len(str(rows[0]["message_excerpt"])), rows[0]["discord_message_id"]) == (500, "disc-1")
