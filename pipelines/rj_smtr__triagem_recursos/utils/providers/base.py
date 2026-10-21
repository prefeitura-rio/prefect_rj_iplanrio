"""
Interface Abstrata para Provedores de Modelos Multimodais (LLM/VLM).

Define o contrato universal de comunicação, parsing tipado com Pydantic
e retorno do consumo real de tokens (response.usage).
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, Optional, Tuple

from ..schemas import EsquemaDecisao


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
    ) -> Tuple[EsquemaDecisao, Dict[str, int]]:
        """Executa a inferência multimodal e retorna a decisão e os tokens reais consumidos.

        :param pdf_bytes: Binário do sub-PDF truncado.
        :param prompt_sistema: Instruções de diretrizes e regras de negócio.
        :param modelo: Identificador específico do modelo (opcional).
        :param temperature: Temperatura de amostragem (0.0 para N=1, 0.7 para N=2).
        :return: Tupla (EsquemaDecisao validado, dicionário de uso de tokens).
        """
        pass
