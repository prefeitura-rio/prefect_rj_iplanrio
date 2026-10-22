from datetime import date, datetime, timezone

import pandas as pd

from pipelines.rj_iplanrio__alertario_previsao_24h.alerting import (
    PrecipitationAlert,
    build_alert_log_rows,
    extract_precipitation_alerts,
    format_precipitation_alert_message,
)


def test_extract_precipitation_alerts_only_keeps_intense_values():
    df = pd.DataFrame(
        [
            {"data_periodo": date(2025, 1, 1), "periodo": "Manhã", "precipitacao": "Sem chuva"},
            {"data_periodo": date(2025, 1, 1), "periodo": "Tarde", "precipitacao": "Chuva moderada a forte isolada"},
            {"data_periodo": date(2025, 1, 1), "periodo": "Noite", "precipitacao": "Chuva fraca"},
            {"data_periodo": date(2025, 1, 2), "periodo": "Manhã", "precipitacao": "Chuva moderada a forte"},
            {"data_periodo": date(2025, 1, 2), "periodo": "Tarde", "precipitacao": "Chuva fraca a moderada"},
            {"data_periodo": date(2025, 1, 3), "periodo": "Manhã", "precipitacao": "Pancadas de chuva"},
            {"data_periodo": date(2025, 1, 3), "periodo": "Tarde", "precipitacao": "Pancadas de chuva isoladas"},
            {"data_periodo": date(2025, 1, 3), "periodo": "Noite", "precipitacao": "Chuva fraca a moderada isolada"},
            {"data_periodo": date(2025, 1, 4), "periodo": "Manhã", "precipitacao": "Chuvisco/Chuva Fraca isolada"},
        ]
    )

    alerts = extract_precipitation_alerts(df)

    assert len(alerts) == 4
    assert {alert.precipitacao for alert in alerts} == {
        "Chuva moderada a forte isolada",
        "Chuva moderada a forte",
        "Pancadas de chuva",
        "Pancadas de chuva isoladas",
    }


def test_format_precipitation_alert_message_matches_expected_layout_with_synoptic():
    alerts = extract_precipitation_alerts(
        pd.DataFrame(
            [
                {"data_periodo": date(2025, 11, 27), "periodo": "Manhã", "precipitacao": "Pancadas de chuva isoladas"},
                {"data_periodo": date(2025, 11, 27), "periodo": "Tarde", "precipitacao": "Pancadas de chuva"},
                {
                    "data_periodo": date(2025, 11, 27),
                    "periodo": "Noite",
                    "precipitacao": "Chuva fraca a moderada isolada",
                },
                {"data_periodo": date(2025, 11, 28), "periodo": "Madrugada", "precipitacao": "Chuva fraca isolada"},
                {"data_periodo": date(2025, 11, 28), "periodo": "Manhã", "precipitacao": "Pancadas de chuva"},
                {"data_periodo": date(2025, 11, 28), "periodo": "Tarde", "precipitacao": "Pancadas de chuva"},
            ]
        )
    )

    synoptic_summary = (
        "As condições atmosféricas serão influenciadas por ventos úmidos do oceano. "
        "Assim, o céu estará com nebulosidade variada e há previsão de chuva."
    )
    formatted = format_precipitation_alert_message(
        alerts,
        synoptic_summary=synoptic_summary,
        synoptic_reference_date=date(2025, 11, 27),
    )
    expected = (
        "⚠️ Previsão de chuva – próximos dias (AlertaRio)\n\n"
        "Quadro sinótico – 27/11/2025\n"
        "As condições atmosféricas serão influenciadas por ventos úmidos do oceano. Assim, o céu estará com nebulosidade variada e há previsão de chuva.\n\n"
        "27/11/2025\n"
        "• Manhã: Pancadas de chuva isoladas\n"
        "• Tarde: Pancadas de chuva\n\n"
        "28/11/2025\n"
        "• Manhã: Pancadas de chuva\n"
        "• Tarde: Pancadas de chuva"
    )

    assert formatted == expected


def test_format_precipitation_alert_message_without_synoptic_keeps_layout():
    alerts = extract_precipitation_alerts(
        pd.DataFrame(
            [
                {"data_periodo": date(2025, 11, 27), "periodo": "Manhã", "precipitacao": "Pancadas de chuva isoladas"},
            ]
        )
    )

    formatted = format_precipitation_alert_message(alerts)
    assert formatted.startswith("⚠️ Previsão de chuva – próximos dias (AlertaRio)\n\n27/11/2025")


def test_build_alert_log_rows_keeps_the_dates_and_the_sent_time():
    alerts = [
        PrecipitationAlert(
            forecast_date=date(2025, 12, 3),
            periodo="Manhã",
            precipitacao="Pancadas de chuva isoladas",
        )
    ]
    rows = build_alert_log_rows(
        alert_date=date(2025, 12, 3),
        id_execucao="abc",
        alert_hash="hash",
        alerts=alerts,
        sent_at=datetime(2025, 12, 3, 12, 0, tzinfo=timezone.utc),
        discord_message_id="disc-1",
        webhook_channel="channel",
        message_excerpt="Mensagem completa",
        severity_level="info",
    )

    assert rows[0]["alert_date"] == date(2025, 12, 3)
    assert rows[0]["forecast_date"] == date(2025, 12, 3)
    assert rows[0]["sent_at"] == datetime(2025, 12, 3, 12, 0, tzinfo=timezone.utc)
