"""
Cliente LLM para OpenAI e Gateways Compatíveis (Bifrost).
Suporta chamadas multimodais para OpenAI, Claude e Gemini via endpoint OpenAI-compatible.
Extrai o consumo exato de tokens da resposta (response.usage).
"""

import base64
import os
from typing import Any, Dict, List, Optional, Tuple
from openai import OpenAI
import pymupdf as fitz

from ...constants import VERTEX_LLM_MODEL
from ..schemas import EsquemaDecisao
from .base import BaseLLMClient


class OpenAILLMClient(BaseLLMClient):
    """Cliente para OpenAI e proxies compatíveis como Bifrost."""

    def __init__(self, default_model: Optional[str] = None) -> None:
        api_key = os.getenv("BIFROST_VIRTUAL_KEY") or os.getenv("OPENAI_API_KEY")
        base_url = os.getenv("BIFROST_BASE_URL")
        modelo_padrao = default_model or os.getenv("LLM_MODEL") or VERTEX_LLM_MODEL

        if not api_key:
            raise ValueError(
                "Chave de API ausente (defina BIFROST_VIRTUAL_KEY ou OPENAI_API_KEY no .env)."
            )

        super().__init__(
            api_key=api_key, base_url=base_url, default_model=modelo_padrao
        )

        # Configuração com timeout estendido para sub-PDFs
        self.client = OpenAI(
            api_key=self.api_key, 
            base_url=self.base_url,
            timeout=180.0,
            max_retries=2
        )

    def _converter_pdf_para_imagens_base64(
        self, pdf_bytes: bytes, dpi: int = 150
    ) -> List[str]:
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        imagens_b64 = []
        zoom = dpi / 72.0
        matriz = fitz.Matrix(zoom, zoom)

        for pagina in doc:
            pix = pagina.get_pixmap(matrix=matriz, alpha=False)
            b64_str = base64.b64encode(pix.tobytes("jpeg")).decode("utf-8")
            imagens_b64.append(b64_str)

        doc.close()
        return imagens_b64

    def classificar(
        self,
        pdf_bytes: bytes,
        prompt_sistema: str,
        modelo: Optional[str] = None,
        temperature: float = 0.0
    ) -> Tuple[EsquemaDecisao, Dict[str, int]]:
        mod = modelo or self.default_model
        pdf_b64 = base64.b64encode(pdf_bytes).decode("utf-8")

        conteudo_usuario: List[Dict[str, Any]] = [
            {
                "type": "text",
                "text": "Analise o processo administrativo e emita a decisão estruturada:",
            },
            {
                "type": "image_url",
                "image_url": {
                    "url": f"data:application/pdf;base64,{pdf_b64}",
                },
            },
        ]

        mensagens = [
            {"role": "system", "content": prompt_sistema},
            {"role": "user", "content": conteudo_usuario},
        ]

        kwargs: Dict[str, Any] = {
            "model": mod,
            "messages": mensagens,
            "response_format": EsquemaDecisao,
            "temperature": temperature,
        }
        if any(r in mod.lower() for r in ["3.7", "o1", "o3"]):
            kwargs["reasoning_effort"] = "medium"

        completion = self.client.beta.chat.completions.parse(**kwargs)

        decisao = completion.choices[0].message.parsed
        if decisao is None:
            raise ValueError(f"Falha na validação do schema no modelo {mod}.")

        usage = getattr(completion, "usage", None)
        prompt_tokens = getattr(usage, "prompt_tokens", 3500) if usage else 3500
        completion_tokens = (
            getattr(usage, "completion_tokens", 150) if usage else 150
        )
        cached_tokens = 0
        if usage and hasattr(usage, "prompt_tokens_details"):
            cached_tokens = getattr(
                usage.prompt_tokens_details, "cached_tokens", 0
            )

        usage_dict = {
            "prompt_tokens": int(prompt_tokens),
            "completion_tokens": int(completion_tokens),
            "cached_tokens": int(cached_tokens),
        }

        return decisao, usage_dict
