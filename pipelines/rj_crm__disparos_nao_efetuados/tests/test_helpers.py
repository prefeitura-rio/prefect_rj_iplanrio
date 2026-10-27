from pipelines.rj_crm__disparos_nao_efetuados.helpers import (
    build_complete_message,
    build_failure_message,
    build_missing_message,
    build_query,
)


def test_the_missing_notice_lists_each_campaign_and_counts_them() -> None:
    """List the campaigns with the HSM and the total."""
    message = build_missing_message(
        [{"nome_campanha": "A", "nome_hsm": "hsm_a"}, {"nome_campanha": "B", "nome_hsm": "hsm_b"}],
        "25/05/2026",
        "https://prefect/run/1",
    )

    assert "(25/05/2026)" in message
    assert "• `A - hsm_a`\n• `B - hsm_b`" in message
    assert message.endswith("Total: **2** campanha(s) sem disparo.\n🔗 https://prefect/run/1")


def test_the_complete_and_the_failure_notices_say_what_happened() -> None:
    """Build the other two notices."""
    assert "(25/05/2026)" in build_complete_message("25/05/2026", None)
    assert build_complete_message("25/05/2026", None).endswith("🔗 URL indisponível")
    failure = build_failure_message("flow", "run", "Failed", "https://prefect/run/1")
    assert "Flow: flow\nFlow Run: run\nState: Failed\n🔗 Link: https://prefect/run/1" in failure


def test_the_query_is_the_template_of_the_pipeline() -> None:
    """Render the query without leaving a template tag."""
    query = build_query()

    assert "campanhas_ativas" in query
    assert "{{" not in query
