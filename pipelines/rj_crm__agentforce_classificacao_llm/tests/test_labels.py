from datetime import UTC, datetime

import pytest
from pydantic import JsonValue

from pipelines.rj_crm__agentforce_classificacao_llm.helpers.labels import (
    build_classified_row,
    build_rule_row,
    check_label,
    check_labels,
    derive_sentiment,
    finish_rows,
    session_columns,
)
from pipelines.rj_crm__agentforce_classificacao_llm.helpers.prompts import split_sessions
from pipelines.rj_crm__agentforce_classificacao_llm.tests.conftest import SessionFactory
from pipelines.rj_crm__agentforce_classificacao_llm.types import Prompted
from utils.bifrost import Answer

NATURES = ("Dúvida", "Problema", "Reclamação", "Elogio", "Solicitação", "Informação")


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        pytest.param("Solicitacao", "Solicitação", id="a_missing_accent_is_fixed"),
        pytest.param("DENTRO_DO_ESCOPO", "DENTRO_DO_ESCOPO", id="an_underscore_matches_a_space"),
        pytest.param("dentro do escopo", "DENTRO_DO_ESCOPO", id="a_space_matches_an_underscore"),
        pytest.param("Dificuldade", None, id="a_label_out_of_the_catalog"),
        pytest.param("  ", None, id="blank"),
        pytest.param(None, None, id="nothing"),
        pytest.param(3, None, id="not_text"),
    ],
)
def test_a_label_goes_back_to_the_spelling_of_the_catalog_or_is_dropped(value: JsonValue, expected: str | None) -> None:
    """Match without accents and capitals."""
    valid = (*NATURES, "DENTRO_DO_ESCOPO")

    assert check_label(value, valid) == expected


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        pytest.param("Dúvida, Problema", "Dúvida, Problema", id="two_valid_labels"),
        pytest.param("duvida,  reclamacao", "Dúvida, Reclamação", id="spelling_and_spaces_are_fixed"),
        pytest.param("Dúvida, Inventada, Dúvida", "Dúvida", id="an_unknown_and_a_repeat_are_dropped"),
        pytest.param("Inventada", None, id="none_valid"),
        pytest.param("", None, id="empty"),
        pytest.param(None, None, id="nothing"),
    ],
)
def test_a_field_with_many_labels_keeps_the_valid_ones_without_repeats(value: JsonValue, expected: str | None) -> None:
    """Check each label of the comma list."""
    assert check_labels(value, NATURES) == expected


@pytest.mark.parametrize(
    ("nature", "expected"),
    [
        pytest.param("Reclamação, Elogio", "negativo", id="a_complaint_weighs_more_than_a_praise"),
        pytest.param("Problema", "negativo", id="a_problem"),
        pytest.param("Elogio, Dúvida", "positivo", id="a_praise"),
        pytest.param("Dúvida, Solicitação", "neutro", id="anything_else"),
        pytest.param(None, None, id="no_nature_is_not_evaluated"),
        pytest.param("", None, id="empty"),
    ],
)
def test_the_sentiment_comes_from_the_nature_by_rule(nature: str | None, expected: str | None) -> None:
    """Give the sentiment, or none when the nature is missing."""
    assert derive_sentiment(nature) == expected


def test_a_session_with_an_hsm_gets_every_label_checked(make_session: SessionFactory) -> None:
    """Fix the labels, drop the invalid ones, and keep the tokens and the raw answer."""
    session = make_session()
    prompted = Prompted(session, "com_hsm", "the prompt")
    parsed: dict[str, JsonValue] = {
        "relacao_hsm": "dentro do escopo",
        "telefone_confirmacao_flag": "pertence",
        "conteudo_relevante": True,
        "resumo": "quer poda",
        "secretarias_relacionadas": "comlurb, Inventada",
        "secretaria_principal": "rioluz",
        "natureza": "Solicitacao",
        "motivo": "m",
        "justificativa": "j",
    }

    row = build_classified_row(prompted, parsed, "model-x", Answer("raw text", 10, 5, 15))

    assert row["relacao_hsm"] == "DENTRO_DO_ESCOPO"
    assert (row["telefone_confirmacao_flag"], row["secretarias_relacionadas"], row["secretaria_principal"]) == (
        "PERTENCE",
        "Comlurb",
        "RioLuz",
    )
    assert (row["natureza"], row["sentimento"], row["conteudo_relevante"]) == ("Solicitação", "neutro", True)
    assert (row["tipo_prompt"], row["modelo"], row["resposta_llm_bruta"], row["prompt_enviado"]) == (
        "com_hsm",
        "model-x",
        "raw text",
        "the prompt",
    )
    assert (row["tokens_entrada"], row["tokens_saida"], row["tokens_total"]) == (10, 5, 15)
    assert row["sessao_fim_datahora"] == "2026-07-17T10:05:00+00:00"


def test_a_session_without_an_hsm_has_no_relation_to_check_and_no_phone_flag(make_session: SessionFactory) -> None:
    """Force `SEM_HSM_ASSOCIADO`.

    An empty relation would mean that it was not classified.
    """
    prompted = Prompted(make_session(hsm_texto=None), "sem_hsm", "p")

    row = build_classified_row(
        prompted,
        {"relacao_hsm": "FORA_DO_ESCOPO", "telefone_confirmacao_flag": "PERTENCE", "conteudo_relevante": "yes"},
        "m",
        Answer("t"),
    )

    assert (row["relacao_hsm"], row["telefone_confirmacao_flag"], row["conteudo_relevante"]) == (
        "SEM_HSM_ASSOCIADO",
        None,
        None,
    )


def test_a_late_reply_to_a_button_is_decided_by_rule_with_no_model(make_session: SessionFactory) -> None:
    """Give the relation, phone flag and note, and leave the model columns empty."""
    row = build_rule_row(make_session())

    assert (row["relacao_hsm"], row["telefone_confirmacao_flag"], row["tipo_prompt"]) == (
        "RESPOSTA_ATRASADA_BTN",
        "PERTENCE",
        None,
    )
    assert all(
        row[key] is None for key in ("modelo", "prompt_enviado", "tokens_total", "resumo", "natureza", "sentimento")
    )
    assert "janela de 24h" in str(row["justificativa"])


def test_the_columns_of_a_session_are_written_as_iso_times(make_session: SessionFactory) -> None:
    """Write the times as text, and nothing as nothing."""
    columns = session_columns(make_session(sessao_inicio_datahora=None))

    assert columns["sessao_inicio_datahora"] is None
    assert columns["hsm_envio_datahora"] == "2026-07-16T09:00:00+00:00"
    assert columns["id_sessao"] == "s1"


def test_the_rows_get_the_columns_of_the_load(make_session: SessionFactory) -> None:
    """Stamp the time, day and prompt version, empty themes and the phone date."""
    with_flag = build_rule_row(make_session())
    without_flag = {**build_rule_row(make_session(id_sessao="s2")), "telefone_confirmacao_flag": None}
    now = datetime(2026, 7, 17, 23, 30, tzinfo=UTC)

    first, second = finish_rows([with_flag, without_flag], "v3", now)

    assert (first["classificado_em"], first["data_particao"], first["prompt_versao"]) == (
        "2026-07-17T23:30:00+00:00",
        "2026-07-17",
        "v3",
    )
    assert (first["tema_nome"], first["causa_nome"]) == ([], [])
    assert first["telefone_confirmacao_datahora"] == "2026-07-17T10:05:00+00:00"
    assert second["telefone_confirmacao_datahora"] is None


def test_the_sessions_split_by_what_happens_to_each(make_session: SessionFactory) -> None:
    """Send HSM and no-HSM sessions to the model, and decide a late reply by rule.

    A session with no talk is dropped.
    """
    sessions = [
        make_session(id_sessao="with_hsm"),
        make_session(id_sessao="no_hsm", hsm_texto=None),
        make_session(id_sessao="late", late_button=True),
        make_session(id_sessao="late_without_talk", late_button=True, mensagens_usuario_concatenadas=""),
        make_session(id_sessao="silent", mensagens_usuario_concatenadas=" "),
        make_session(id_sessao="silent_no_hsm", hsm_texto="", mensagens_usuario_concatenadas=None),
    ]

    split = split_sessions(sessions)

    assert [(item.session.id_sessao, item.kind) for item in split.prompted] == [
        ("with_hsm", "com_hsm"),
        ("no_hsm", "sem_hsm"),
    ]
    assert [session.id_sessao for session in split.by_rule] == ["late", "late_without_talk"]
    assert split.dropped == 2
    assert "olá" in split.prompted[0].prompt
    assert "CIDADÃO: quero ajuda" in split.prompted[0].prompt
    assert "<<" not in split.prompted[1].prompt
