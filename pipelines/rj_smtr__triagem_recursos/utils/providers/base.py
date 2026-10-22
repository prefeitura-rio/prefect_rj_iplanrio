"""Interface Abstrata para Provedores de Modelos Multimodais (LLM/VLM).

Define o contrato universal de comunicação, parsing tipado com Pydantic
e retorno do consumo real de tokens (response.usage).
"""

from abc import ABC, abstractmethod

from ..schemas import EsquemaDecisao


class BaseLLMClient(ABC):
    """Contrato base para clientes de inferência multimodal."""

    def __init__(
        self,
        api_key: str | None = None,
        base_url: str | None = None,
        default_model: str | None = None,
    ) -> None:
        self.api_key = api_key
        self.base_url = base_url
        self.default_model = default_model

    @abstractmethod
    def classificar(
        self,
        pdf_bytes: bytes,
        prompt_sistema: str,
        modelo: str | None = None,
        temperature: float = 0.0,
        reasoning_effort: str = "medium",
    ) -> tuple[EsquemaDecisao, dict[str, int]]:
        """Executa a inferência multimodal e retorna a decisão e os tokens reais consumidos.

        :param pdf_bytes: Binário do sub-PDF truncado.
        :param prompt_sistema: Instruções de diretrizes e regras de negócio.
        :param modelo: Identificador específico do modelo (opcional).
        :param temperature: Temperatura de amostragem (0.0 para N=1, 0.7 para N=2).
        :param reasoning_effort: Nível de esforço de raciocínio da LLM.
        :returns: Tupla contendo EsquemaDecisao validado e dicionário de uso de tokens.
        """
        pass
