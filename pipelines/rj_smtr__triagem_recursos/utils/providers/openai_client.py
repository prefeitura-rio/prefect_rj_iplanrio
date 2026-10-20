"""
Cliente LLM para OpenAI e Gateways Compatíveis (Bifrost).
Suporta chamadas multimodais para OpenAI, Claude e Gemini via endpoint OpenAI-compatible.
"""

import base64
import os
from typing import Any, Dict, List, Optional
from openai import OpenAI
import pymupdf as fitz

from ...constants import VERTEX_LLM_MODEL
from utils.schemas import EsquemaDecisao
from .base import BaseLLMClient


class OpenAILLMClient(BaseLLMClient):
    """Cliente para OpenAI e proxies compatíveis como Bifrost."""

    def __init__(self, default_model: Optional[str] = None) -> None:
        api_key = os.getenv("BIFROST_VIRTUAL_KEY") or os.getenv("OPENAI_API_KEY")
        base_url = os.getenv("BIFROST_BASE_URL")
        modelo_padrao = default_model or os.getenv("LLM_MODEL") or VERTEX_LLM_MODEL

        if not api_key:
            raise ValueError("Chave de API ausente (defina BIFROST_VIRTUAL_KEY ou OPENAI_API_KEY no .env).")

        super().__init__(api_key=api_key, base_url=base_url, default_model=modelo_padrao)

        # Configuração com timeout estendido para PDFs de 20 páginas
        self.client = OpenAI(
            api_key=self.api_key, 
            base_url=self.base_url,
            timeout=180.0,
            max_retries=2
        )

    def _converter_pdf_para_imagens_base64(self, pdf_bytes: bytes, dpi: int = 150) -> List[str]:
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
    ) -> EsquemaDecisao:
        mod = modelo or self.default_model
        imagens_b64 = self._converter_pdf_para_imagens_base64(pdf_bytes)

        conteudo_usuario: List[Dict[str, Any]] = [
            {"type": "text", "text": "Analise o processo administrativo e emita a decisão estruturada:"}
        ]

        for img_b64 in imagens_b64:
            conteudo_usuario.append({
                "type": "image_url",
                "image_url": {"url": f"data:image/jpeg;base64,{img_b64}", "detail": "high"}
            })

        mensagens = [
            {"role": "system", "content": prompt_sistema},
            {"role": "user", "content": conteudo_usuario}
        ]

        completion = self.client.beta.chat.completions.parse(
            model=mod,
            messages=mensagens,
            response_format=EsquemaDecisao,
            temperature=temperature
        )
        
        decisao = completion.choices[0].message.parsed
        if decisao is None:
            raise ValueError(f"Falha na validação do schema no modelo {mod}.")

        return decisao