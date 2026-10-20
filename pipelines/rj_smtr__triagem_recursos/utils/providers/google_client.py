"""
Cliente de Inferência Multimodal para Provedor Google GenAI / Vertex AI.

Utiliza a ingestão nativa de sub-PDFs em bytes (application/pdf) com resolução padrão,
garantindo menor consumo de tokens e resposta estruturada via SDK oficial google-genai.
"""

import os
from typing import Optional
from google import genai
from google.genai import types

from utils.schemas import EsquemaDecisao
from .base import BaseLLMClient


class GoogleLLMClient(BaseLLMClient):
    """Implementação do cliente para a API do Google Gemini."""

    def __init__(
        self,
        api_key: Optional[str] = None,
        base_url: Optional[str] = None,
        default_model: Optional[str] = None
    ) -> None:
        key = api_key or os.getenv("GEMINI_API_KEY")
        if not key:
            raise ValueError("Chave GEMINI_API_KEY não informada ou ausente no .env.")

        model = default_model or os.getenv("LLM_MODEL", "gemini-3.7-flash")
        super().__init__(api_key=key, base_url=base_url, default_model=model)
        
        self.client = genai.Client(api_key=self.api_key)

    def classificar(
        self,
        pdf_bytes: bytes,
        prompt_sistema: str,
        modelo: Optional[str] = None
    ) -> EsquemaDecisao:
        """Envia os bytes do PDF diretamente ao Gemini e valida a saída estruturada."""
        model_name = modelo or self.default_model

        response = self.client.models.generate_content(
            model=model_name,
            contents=[
                types.Part.from_bytes(data=pdf_bytes, mime_type="application/pdf"),
                "Analise as páginas do processo de recurso de trânsito em anexo e execute a triagem estrita conforme as instruções."
            ],
            config=types.GenerateContentConfig(
                system_instruction=prompt_sistema,
                response_mime_type="application/json",
                response_schema=EsquemaDecisao,
                temperature=0.1,
            ),
        )
        
        prompt_tokens = response.usage_metadata.prompt_token_count
        completion_tokens = response.usage_metadata.candidates_token_count
        
        return EsquemaDecisao.model_validate_json(response.text), prompt_tokens, completion_tokens