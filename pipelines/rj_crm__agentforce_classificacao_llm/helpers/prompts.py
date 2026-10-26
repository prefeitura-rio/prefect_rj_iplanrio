"""The prompts of the model, and which sessions get one."""

from collections.abc import Sequence
from functools import cache
from pathlib import Path

from pipelines.rj_crm__agentforce_classificacao_llm.constants import WITH_HSM, WITHOUT_HSM
from pipelines.rj_crm__agentforce_classificacao_llm.types import Prompted, Session, Split

PROMPTS = Path(__file__).parent.parent / "prompts"


@cache
def read_prompt(name: str) -> str:
    """Return the text of `prompts/<name>.txt`."""
    return (PROMPTS / f"{name}.txt").read_text(encoding="utf-8")


def fill_prompt(template: str, hsm_text: str | None, conversation: str | None) -> str:
    """Return the template with the HSM text and the conversation in place.

    Nothing becomes empty text.
    """
    return template.replace("<<HSM_TEXTO>>", hsm_text or "").replace("<<CONVERSA_COMPLETA>>", conversation or "")


def split_sessions(sessions: Sequence[Session]) -> Split:
    """Split the sessions into the ones for the model, the ones that a rule decides, and the ones dropped.

    - With an HSM and not a late reply to a button, the session goes to the model with
      the `com_hsm` prompt.
    - With no HSM, it goes with the `sem_hsm` prompt.
    - With an HSM and a late reply to a button, a rule decides, and the model does not
      run.
    - A session with no message of the citizen is dropped. It is extracted again at the
      next run, inside the window, at no cost for the model.
    """
    prompted: list[Prompted] = []
    by_rule: list[Session] = []

    for session in sessions:
        has_hsm = bool((session.hsm_texto or "").strip())
        has_talk = bool((session.mensagens_usuario_concatenadas or "").strip())

        if has_hsm and session.late_button:
            by_rule.append(session)
        elif has_hsm and has_talk:
            template = read_prompt("classificacao_hsm")
            prompted.append(
                Prompted(session, WITH_HSM, fill_prompt(template, session.hsm_texto, session.conversa_completa))
            )
        elif has_talk:
            template = read_prompt("classificacao_sem_hsm")
            prompted.append(Prompted(session, WITHOUT_HSM, fill_prompt(template, None, session.conversa_completa)))

    return Split(prompted, by_rule, len(sessions) - len(prompted) - len(by_rule))
