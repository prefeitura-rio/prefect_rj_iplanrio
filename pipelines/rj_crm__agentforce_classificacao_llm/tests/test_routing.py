import pytest

from pipelines.rj_crm__agentforce_classificacao_llm.helpers.routing import (
    candidate_secretariats,
    check_pair,
    macrotheme_prompt,
    secretariat_prompt,
)


def test_macrotheme_catalog_has_no_comma_in_a_label() -> None:
    """Keep comma available as the related-label separator."""
    prompt = macrotheme_prompt("hsm", "talk")

    assert "Assistência social / benefícios" in prompt
    assert "<<MACROTEMAS>>" not in prompt


@pytest.mark.parametrize(
    ("related", "expected"),
    [
        pytest.param(
            "Servidor municipal",
            ("Comlurb", "GBP", "GM-Rio", "PREVI-RIO", "RioSaúde", "SME", "SMS", "SUBGGC"),
            id="active-servant-pay-includes-subggc",
        ),
        pytest.param("Outros", None, id="macrotheme-without-candidates-uses-all"),
        pytest.param(None, None, id="no-macrotheme-uses-all"),
    ],
)
def test_secretariat_candidates_come_from_the_macrotheme(related: str | None, expected: tuple[str, ...] | None) -> None:
    """Return the union of candidates, or the full-list mark."""
    assert candidate_secretariats(related) == expected


def test_the_principal_label_is_added_to_related_labels() -> None:
    """Keep the principal label in the related set."""
    assert check_pair("Servidor municipal", "Saúde", ["Servidor municipal", "Saúde"]) == (
        "Servidor municipal",
        "Servidor municipal, Saúde",
    )


def test_filtered_secretariat_prompt_has_outside_option_and_stable_prefix() -> None:
    """Render candidates and the retry marker without placeholders."""
    prompt = secretariat_prompt(("SUBGGC", "PREVI-RIO"), "hsm", "talk")

    assert "SUBGGC" in prompt
    assert "PREVI-RIO" in prompt
    assert "OUTRA" in prompt
    assert "<<SECRETARIAS>>" not in prompt
    assert "<<REGRA_FORA_DA_LISTA>>" not in prompt
