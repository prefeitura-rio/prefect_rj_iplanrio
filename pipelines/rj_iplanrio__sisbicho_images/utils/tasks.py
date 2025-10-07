# -*- coding: utf-8 -*-
import base64

from iplanrio.pipelines_utils.logging import log

# Mapeamento de magic numbers
MAGIC_NUMBERS = {
    b"\x89PNG\r\n\x1a\n": "png",
    b"\xff\xd8\xff": "jpeg",
    b"GIF87a": "gif",
    b"GIF89a": "gif",
}


def detect_and_decode(data_b64: str) -> bytes:
    """Detecta se o Base64 precisa de 1 ou 2 decodificações e retorna os bytes da imagem."""
    data_b64 = data_b64.strip()
    # Primeira tentativa
    try:
        step1 = base64.b64decode(data_b64, validate=True)
    except Exception:
        raise ValueError("Base64 inválido na primeira tentativa")

    # Checa se bate com algum magic number
    if any(step1.startswith(m) for m in MAGIC_NUMBERS):
        log("Decodificação única suficiente.")
        return step1

    # Segunda tentativa
    try:
        step2 = base64.b64decode(step1, validate=True)
    except Exception:
        raise ValueError("Base64 inválido na segunda tentativa")

    if any(step2.startswith(m) for m in MAGIC_NUMBERS):
        log("Decodificação dupla necessária.")
        return step2

    raise ValueError("Não foi possível identificar o tipo de arquivo após 1 ou 2 decodificações.")
