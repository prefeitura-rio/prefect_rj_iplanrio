"""
Interface Abstrata para Provedores de Modelos Multimodais (LLM/VLM).

Define o contrato universal de comunicação e parsing tipado com Pydantic,
garantindo independência de fornecedor (Google Cloud, OpenAI, Bifrost Gateway).
"""

from abc import ABC, abstractmethod
from typing import Optional


class BaseLLMClient(ABC):
    """Contrato base para clientes de inferência multimodal."""

    def __init__(
        self, 
        api_key: Optional[str] = None, 
        base_url: Optional[str] = None, 
        default_model: Optional[str] = None
    ) -> None:
        self.api_key = api_key
        self.base_url = base_url
        self.default_model = default_model

    @abstractmethod
    def classificar(
        self, 
        pdf_bytes: bytes, 
        prompt_sistema: str, 
        modelo: Optional[str] = None,
        temperature: float = 0.0
    ):
        """Executa a inferência multimodal e retorna o esquema estruturado.

        Args:
            pdf_bytes: Binário do sub-PDF truncado.
            prompt_sistema: Instruções de diretrizes e regras de negócio.
            modelo: Identificador específico do modelo (opcional).
            temperature: Temperatura de amostragem (0.0 para N=1, 0.7 para N=2).

        Returns:
            EsquemaDecisao: Objeto validado pelo Pydantic.
        """
        pass