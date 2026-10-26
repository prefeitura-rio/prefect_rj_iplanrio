"""The conversations of a dispatch, from the messages of its sessions."""

from collections.abc import Sequence
from datetime import UTC, datetime
from itertools import groupby

from pipelines.rj_crm__relatorio_engajamento_hsm.constants import CITIZEN_SOURCE, HSM_SOURCE, SOURCE_LABELS
from pipelines.rj_crm__relatorio_engajamento_hsm.types import Conversation, Extraction, Message


def message_order(message: Message) -> tuple[str, bool, datetime]:
    """Return the key that orders messages by session and time, with a message that has no time last."""
    moment = message["msg_data"]

    return message["id_sessao_48h"], moment is None, moment or datetime.min.replace(tzinfo=UTC)


def build_conversations(messages: Sequence[Message]) -> Extraction:
    """Group the messages by session, and return a conversation for each one that has a citizen message.

    A conversation has the lines `CIDADÃO: ...` and `AGENTE: ...` in time order. The HSM
    text is kept apart as `hsm_text`, the same for all the sessions of a dispatch, and
    does not repeat in the conversation. The totals count all the sessions, so the
    report can give the rate of engagement.
    """
    ordered = sorted(messages, key=message_order)
    sessions = [list(group) for _, group in groupby(ordered, key=lambda message: message["id_sessao_48h"])]
    conversations: list[Conversation] = []

    for session in sessions:
        if not any(message["msg_fonte"] == CITIZEN_SOURCE for message in session):
            continue

        first = session[0]
        hsm_text = next((message["msg_texto"] or "" for message in session if message["msg_fonte"] == HSM_SOURCE), "")
        text = "\n".join(
            f"{SOURCE_LABELS.get(message['msg_fonte'], message['msg_fonte'])}: {message['msg_texto'] or ''}"
            for message in session
            if message["msg_fonte"] != HSM_SOURCE
        )
        conversations.append(
            Conversation(
                session_id=first["id_sessao_48h"],
                cpf=first["cpf"],
                phone=first["telefone"],
                campaign=first["nome_campanha"],
                axis=first["nome_eixo"],
                hsm_text=hsm_text,
                text=text,
            )
        )

    return Extraction(conversations, len(sessions), len(conversations))
