"""Identificador da configuração que produziu um resultado (``versao_processamento``)."""

import hashlib
import json

from .. import constants
from .prompts import PromptSet

AUTO_PREFIX = "auto-"


def compute_processing_version(prompts: PromptSet, override: str | None = None) -> str:
    """Calcula a ``versao_processamento`` de uma submissão.

    Sem ``override``, é o hash de tudo que muda o resultado do modelo: modelo,
    configuração de geração e o texto dos dois prompts. Mudanças só de código de
    pós-processamento exigem passar um ``override``.

    :param prompts: Prompts usados na submissão.
    :param override: Rótulo informado no parâmetro do flow, ou ``None``.
    :returns: O rótulo informado, ou ``auto-<12 hex>``.
    :raises ValueError: Se ``override`` for vazio.
    """
    if override is not None:
        label = override.strip()
        if not label:
            raise ValueError("versao_processamento não pode ser vazia.")
        return label
    fingerprint = {
        "model": constants.MODEL_NAME,
        "generation_config": constants.GENERATION_CONFIG,
        "classification_prompt": prompts.classification_text,
        "extraction_prompt": prompts.extraction_text,
    }
    payload = json.dumps(fingerprint, sort_keys=True, ensure_ascii=False).encode("utf-8")
    return AUTO_PREFIX + hashlib.sha256(payload).hexdigest()[:12]
