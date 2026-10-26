"""The labels that the model answers, checked against the catalog, and what comes out of an answer."""

from collections.abc import Mapping, Sequence
from datetime import UTC, datetime

from pydantic import JsonValue

from pipelines.rj_crm__agentforce_classificacao_llm.constants import (
    CLASS_LATE_BUTTON,
    CLASS_WITHOUT_HSM,
    LATE_BUTTON_NOTE,
    VALID_NATURES,
    VALID_PHONE_FLAGS,
    VALID_RELATIONS,
    VALID_SECRETARIATS,
    WITH_HSM,
    WITHOUT_HSM,
)
from pipelines.rj_crm__agentforce_classificacao_llm.helpers.hsm import normalize_text
from pipelines.rj_crm__agentforce_classificacao_llm.types import Prompted, Row, Session
from utils.bifrost import Answer


def normalize_label(text: object) -> str:
    """Return a label to compare: no accents, no capitals, `_` as a space and one space between words."""
    return normalize_text(str(text).replace("_", " "))


def check_label(value: JsonValue, valid: Sequence[str]) -> str | None:
    """Return the label of the catalog that the value matches, with accents and capitals ignored, or nothing.

    A value that is empty or in none of the labels gives nothing. A label out of the
    catalog would break the count by value in any later analysis, so it is dropped.
    """
    if not isinstance(value, str) or not value.strip():
        return None

    return {normalize_label(label): label for label in valid}.get(normalize_label(value))


def check_labels(value: JsonValue, valid: Sequence[str]) -> str | None:
    """Return the labels of a field that has many, separated by commas, each checked and without repeats.

    A label that is not in the catalog is dropped and the others stay. Return nothing if
    none is left.
    """
    if not isinstance(value, str) or not value.strip():
        return None

    found = dict.fromkeys(label for part in value.split(",") if (label := check_label(part, valid)))

    return ", ".join(found) if found else None


def derive_sentiment(nature: str | None) -> str | None:
    """Return the sentiment from the nature of the manifestation, by rule and not by the model.

    A complaint or a problem is negative, and weighs more than a praise. A praise alone
    is positive, and any other nature is neutral. Without a nature there is no
    sentiment: it was not evaluated.
    """
    if not nature:
        return None

    if "Reclamação" in nature or "Problema" in nature:
        return "negativo"

    return "positivo" if "Elogio" in nature else "neutro"


def session_columns(session: Session) -> Row:
    """Return the columns of a session that reach the table."""
    return {
        "id_sessao": session.id_sessao,
        "telefone": session.telefone,
        "cpf": session.cpf,
        "nome_cidadao": session.nome_cidadao,
        "sessao_inicio_datahora": session.sessao_inicio_datahora.isoformat()
        if session.sessao_inicio_datahora
        else None,
        "sessao_fim_datahora": session.sessao_fim_datahora.isoformat() if session.sessao_fim_datahora else None,
        "jornada_nome": session.jornada_nome,
        "id_jornada": session.id_jornada,
        "id_disparo_hsm": session.id_disparo_hsm,
        "hsm_envio_datahora": session.hsm_envio_datahora.isoformat() if session.hsm_envio_datahora else None,
        "hsm_texto": session.hsm_texto,
        "conversa_completa": session.conversa_completa,
    }


def build_classified_row(prompted: Prompted, parsed: Mapping[str, JsonValue], model: str, answer: Answer) -> Row:
    """Return the row of a session that the model classified, with every label checked.

    A `sem_hsm` session has no HSM to compare, so its relation is `SEM_HSM_ASSOCIADO`
    and not nothing, which would mean that it was not classified yet. The confirmation
    of the phone exists only with an HSM.
    """
    kind = prompted.kind
    nature = check_labels(parsed.get("natureza"), VALID_NATURES)

    return {
        **session_columns(prompted.session),
        "tipo_prompt": kind,
        "relacao_hsm": CLASS_WITHOUT_HSM
        if kind == WITHOUT_HSM
        else check_label(parsed.get("relacao_hsm"), VALID_RELATIONS),
        "telefone_confirmacao_flag": (
            check_label(parsed.get("telefone_confirmacao_flag"), VALID_PHONE_FLAGS) if kind == WITH_HSM else None
        ),
        "conteudo_relevante": parsed.get("conteudo_relevante")
        if isinstance(parsed.get("conteudo_relevante"), bool)
        else None,
        "resumo": parsed.get("resumo"),
        "secretarias_relacionadas": check_labels(parsed.get("secretarias_relacionadas"), VALID_SECRETARIATS),
        "secretaria_principal": check_label(parsed.get("secretaria_principal"), VALID_SECRETARIATS),
        "natureza": nature,
        "sentimento": derive_sentiment(nature),
        "motivo": parsed.get("motivo"),
        "justificativa": parsed.get("justificativa"),
        "resposta_llm_bruta": answer.text,
        "modelo": model,
        "prompt_enviado": prompted.prompt,
        "tokens_entrada": answer.prompt_tokens,
        "tokens_saida": answer.output_tokens,
        "tokens_total": answer.total_tokens,
    }


def build_rule_row(session: Session) -> Row:
    """Return the row of a session that a rule decided: a late reply to a button, with no call to the model.

    Clicking a button of the HSM, even late, is engagement with its subject, so the
    phone belongs to the citizen. The model, the prompt, the tokens and the type of
    prompt stay empty, because no model ran.
    """
    return {
        **session_columns(session),
        "tipo_prompt": None,
        "relacao_hsm": CLASS_LATE_BUTTON,
        "telefone_confirmacao_flag": "PERTENCE",
        "conteudo_relevante": None,
        "resumo": None,
        "secretarias_relacionadas": None,
        "secretaria_principal": None,
        "natureza": None,
        "sentimento": None,
        "motivo": None,
        "justificativa": LATE_BUTTON_NOTE,
        "resposta_llm_bruta": None,
        "modelo": None,
        "prompt_enviado": None,
        "tokens_entrada": None,
        "tokens_saida": None,
        "tokens_total": None,
    }


def finish_rows(rows: Sequence[Row], prompt_version: str, now: datetime) -> list[Row]:
    """Return the rows with the columns of the load: the time and day of the classification and the prompt version.

    The date of the phone confirmation is the end of the session, kept only when the
    flag is there. The theme and the cause start empty, because the rules fill them
    next.
    """
    stamp = now.astimezone(UTC)

    return [
        {
            **row,
            "classificado_em": stamp.isoformat(),
            "data_particao": stamp.date().isoformat(),
            "prompt_versao": prompt_version,
            "telefone_confirmacao_datahora": row["sessao_fim_datahora"]
            if row.get("telefone_confirmacao_flag")
            else None,
            "tema_nome": [],
            "causa_nome": [],
        }
        for row in rows
    ]
