# -*- coding: utf-8 -*-
"""Cliente Bifrost (gateway LLM) — self-contido, sem dependência de config específica
de uma etapa (descoberta/classificação/juiz passam a URL/modelo/chave que quiserem)."""

from __future__ import annotations

import math
import time

import requests

from pipelines.rj_crm__relatorio_engajamento_hsm.config import (
    BIFROST_BASE_URL,
    BIFROST_MODEL,
    ESPERA_INICIAL_SEGUNDOS,
    MAX_OUTPUT_TOKENS_LOTE,
    MAX_TENTATIVAS_LLM,
)

# Baseline de calibração de max_output_tokens_para — o tamanho de lote pro qual
# MAX_OUTPUT_TOKENS_LOTE (8192) foi medido como suficiente. FIXO de propósito, mesmo
# que tamanho_lote_classificacao vire parâmetro do flow (config.py) e alguém rode com
# um valor diferente de 50 — a calibração é sobre a REAL relação tokens/item, não sobre
# o que o usuário escolheu rodar hoje.
_TAMANHO_LOTE_CALIBRACAO = 50


class BifrostClient:
    def __init__(self, api_key: str, base: str = BIFROST_BASE_URL, model: str = BIFROST_MODEL, timeout: int = 120):
        self.api_key = api_key
        self.base = base
        self.model = model
        self.timeout = timeout
        self._session = requests.Session()

    def ask(
        self,
        prompt: str,
        system_instruction: str = "Responda em PT-BR.",
        max_output_tokens: int = 512,
        temperature: float = 0.0,
        max_tentativas: int = MAX_TENTATIVAS_LLM,
        espera_inicial: int = ESPERA_INICIAL_SEGUNDOS,
    ) -> dict:
        payload = {
            "systemInstruction": {"parts": [{"text": system_instruction}]},
            "contents": [{"role": "user", "parts": [{"text": prompt}]}],
            "generationConfig": {
                "maxOutputTokens": max_output_tokens,
                "temperature": temperature,
                "thinkingConfig": {"thinkingBudget": 0, "includeThoughts": True},
            },
        }
        ultimo_erro = None
        for tentativa in range(1, max_tentativas + 1):
            try:
                return self._chama(payload)
            except RuntimeError as e:
                ultimo_erro = e
                if tentativa < max_tentativas:
                    time.sleep(espera_inicial * (2 ** (tentativa - 1)))
        raise ultimo_erro

    def _chama(self, payload: dict) -> dict:
        url = f"{self.base}/genai/v1beta/models/{self.model}:generateContent"
        headers = {"Content-Type": "application/json", "x-goog-api-key": self.api_key}
        try:
            resp = self._session.post(url, headers=headers, json=payload, timeout=self.timeout)
        except requests.RequestException as e:
            raise RuntimeError(f"requisição ao Bifrost falhou: {e}") from e
        try:
            parsed = resp.json()
        except ValueError as e:
            resp.raise_for_status()
            raise RuntimeError(f"Bifrost retornou corpo não-JSON (status {resp.status_code})") from e
        if isinstance(parsed, dict) and "error" in parsed:
            erro = parsed["error"]
            raise RuntimeError(f"Bifrost/API retornou erro {erro.get('code')} ({erro.get('status')}): {erro.get('message')}")
        resp.raise_for_status()
        return parsed

    @staticmethod
    def extract_text(response: dict) -> str:
        parts = response["candidates"][0]["content"]["parts"]
        return "".join(p["text"] for p in parts if not p.get("thought"))


def max_output_tokens_para(tamanho_lote: int) -> int:
    """Escala o teto de tokens de saída proporcional ao tamanho do lote — sem isso, um
    tamanho de lote maior que o default arrisca cortar a resposta da LLM no meio (JSON
    inválido, lote inteiro falha)."""
    return max(MAX_OUTPUT_TOKENS_LOTE, math.ceil(MAX_OUTPUT_TOKENS_LOTE * tamanho_lote / _TAMANHO_LOTE_CALIBRACAO))
