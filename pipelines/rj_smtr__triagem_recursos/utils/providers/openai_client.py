"""Cliente LLM para OpenAI e Gateways Compatíveis (Bifrost).

Suporta chamadas multimodais para OpenAI, Claude e Gemini via endpoint OpenAI-compatible.
Extrai o consumo exato de tokens da resposta (response.usage).
"""

import base64
import os
from openai import OpenAI
import pymupdf as fitz

from ...constants import VERTEX_LLM_MODEL
from ..schemas import EsquemaDecisao
from .base import BaseLLMClient


class OpenAILLMClient(BaseLLMClient):
    """Cliente para OpenAI e proxies compatíveis como Bifrost."""

    def __init__(self, default_model: str | None = None) -> None:
        api_key = os.getenv("BIFROST_VIRTUAL_KEY") or os.getenv("OPENAI_API_KEY")
        base_url = os.getenv("BIFROST_BASE_URL")
        modelo_padrao = (
            default_model or os.getenv("LLM_MODEL") or VERTEX_LLM_MODEL
        )

        if not api_key:
            raise ValueError(
                "Chave de API ausente (defina BIFROST_VIRTUAL_KEY ou OPENAI_API_KEY no .env)."
            )

        super().__init__(
            api_key=api_key, base_url=base_url, default_model=modelo_padrao
        )

        self.client = OpenAI(
            api_key=self.api_key,
            base_url=self.base_url,
            timeout=180.0,
            max_retries=2,
        )

    def converter_pdf_para_imagens_base64(
        self, pdf_bytes: bytes, dpi: int = 150
    ) -> list[str]:
        """Converte páginas de PDF em imagens JPEG serializadas em Base64.

        :param pdf_bytes: Stream de bytes do PDF de entrada.
        :param dpi: Resolução de rasterização.
        :returns: Lista de strings em Base64 correspondentes a cada página.
        """
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
        modelo: str | None = None,
        temperature: float = 0.0,
        reasoning_effort: str = "medium",
    ) -> tuple[EsquemaDecisao, dict[str, int]]:
        """Executa a inferência multimodal no gateway OpenAI/Bifrost.

        :param pdf_bytes: Binário do sub-PDF truncado.
        :param prompt_sistema: Instruções de diretrizes e regras de negócio.
        :param modelo: Identificador específico do modelo (opcional).
        :param temperature: Temperatura de amostragem.
        :param reasoning_effort: Nível de esforço de raciocínio.
        :returns: Tupla contendo EsquemaDecisao validado e dicionário de uso de tokens.
        """
        target_model = modelo or self.default_model

        imagens_b64 = self.converter_pdf_para_imagens_base64(pdf_bytes, dpi=150)

        conteudo_usuario = [
            {
                "type": "text",
                "text": "Analise as páginas do processo de recurso de trânsito em anexo e execute a triagem estrita conforme as instruções.",
            }
        ]

        for img_b64 in imagens_b64:
            conteudo_usuario.append({
                "type": "image_url",
                "image_url": {
                    "url": f"data:image/jpeg;base64,{img_b64}",
                    "detail": "high",
                },
            })

        extra_body = {}
        if reasoning_effort and reasoning_effort != "none":
            extra_body["reasoning_effort"] = reasoning_effort

        response = self.client.beta.chat.completions.parse(
            model=target_model,
            messages=[
                {"role": "system", "content": prompt_sistema},
                {"role": "user", "content": conteudo_usuario},
            ],
            response_format=EsquemaDecisao,
            temperature=temperature,
            extra_body=extra_body if extra_body else None,
        )

        usage = {"prompt_tokens": 0, "completion_tokens": 0, "cached_tokens": 0}
        if response.usage:
            cached = 0
            if hasattr(response.usage, "prompt_tokens_details") and response.usage.prompt_tokens_details:
                cached = getattr(
                    response.usage.prompt_tokens_details, "cached_tokens", 0
                )

            usage = {
                "prompt_tokens": response.usage.prompt_tokens,
                "completion_tokens": response.usage.completion_tokens,
                "cached_tokens": cached or 0,
            }

        return response.choices[0].message.parsed, usage
