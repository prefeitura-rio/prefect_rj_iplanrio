"""The prompts of the model."""

from collections.abc import Sequence
from functools import cache
from pathlib import Path

from pipelines.rj_crm__relatorio_engajamento_hsm.types import Classification, Conversation

PROMPTS = Path(__file__).parent.parent / "prompts"


@cache
def read_prompt(name: str) -> str:
    """Return the text of `prompts/<name>.txt`."""
    return (PROMPTS / f"{name}.txt").read_text(encoding="utf-8")


def first_hsm_text(conversations: Sequence[Conversation]) -> str:
    """Return the HSM text of the first conversation that has one, or empty text."""
    return next((conversation.hsm_text for conversation in conversations if conversation.hsm_text), "")


def fill_prompt(template: str, hsm_text: str, catalog_text: str, conversations_text: str) -> str:
    """Return the template with the HSM text, catalog and conversations in place."""
    return (
        template.replace("<<HSM_TEXTO>>", hsm_text)
        .replace("<<CATALOGO_CATEGORIAS>>", catalog_text)
        .replace("<<CONVERSAS>>", conversations_text)
    )


def format_conversations(batch: Sequence[Conversation]) -> str:
    """Return a batch as numbered blocks, from 1, each with the id of its session."""
    return "\n\n".join(
        f"### CONVERSA {number} (id_sessao_48h={conversation.session_id})\n{conversation.text}"
        for number, conversation in enumerate(batch, start=1)
    )


def format_judged(batch: Sequence[Classification]) -> str:
    """Return a batch for the judge: each conversation with its category and reason."""
    blocks = [
        "\n".join(
            [
                f"### CONVERSA {number} (id_sessao_48h={item.conversation.session_id})",
                item.conversation.text,
                f"Categoria proposta: {item.category}",
                f"Justificativa proposta: {item.justification}",
            ]
        )
        for number, item in enumerate(batch, start=1)
    ]

    return "\n\n".join(blocks)
