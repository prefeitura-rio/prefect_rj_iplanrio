# -*- coding: utf-8 -*-
"""Helpers de texto puros, sem estado — usados por várias etapas (descoberta,
classificação, juiz, relatório)."""

from __future__ import annotations

import re
import unicodedata


def hsm_sane(nome_hsm: str) -> str:
    """Nome do HSM sanitizado pra usar em nome de arquivo/pasta."""
    return re.sub(r"[^A-Za-z0-9]+", "_", nome_hsm).strip("_").lower()


def normaliza_rotulo(texto) -> str:
    """Normaliza nome de categoria pra comparação (evita duplicar por acento/maiúscula/
    espaço vs underscore) — usado por CatalogoCategorias (utils/catalogo.py)."""
    texto = str(texto).strip().lower().replace("_", " ")
    texto = unicodedata.normalize("NFKD", texto).encode("ascii", "ignore").decode("ascii")
    return re.sub(r"\s+", " ", texto)


def strip_code_fence(texto: str) -> str:
    """Remove ```json ... ``` (ou variante sem linguagem) que a LLM às vezes envolve a
    resposta com, mesmo pedindo JSON puro no prompt."""
    texto = texto.strip()
    if texto.startswith("```"):
        texto = re.sub(r"^```(\w+)?\s*|\s*```$", "", texto, flags=re.IGNORECASE).strip()
    return texto
