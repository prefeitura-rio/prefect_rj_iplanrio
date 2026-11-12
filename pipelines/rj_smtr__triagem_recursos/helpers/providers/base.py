"""Interface abstrata universal para clientes de provedores multimodais (LLM/VLM).

Descrição Técnica & Arquitetura:
Estabelece o contrato formal de inferência para qualquer cliente de modelo de linguagem
utilizado na pipeline de triagem (Bifrost, OpenAI ou Google Vertex AI). Exige implementação
concreta do método `classificar`, assegurando que a resposta seja invariavelmente convertida
para a estrutura canônica `EsquemaDecisao` e que a contagem exata de tokens utilizados
seja reportada para telemetria de custos e auditoria de Prompt Caching.

Fluxo de Dependências:
- Consumidores Diretos:
  * `utils/providers/openai_client.py`: herda desta classe base e implementa a comunicação via Bifrost/OpenAI.
  * `utils/providers/google_client.py`: herda desta classe base e implementa a comunicação via SDK Vertex AI.
  * `utils/triage.py`: tipagem genérica para injeção de dependência de clientes LLM.
- Dependências Diretas:
  * `utils/schemas.py`: importa `EsquemaDecisao` para definição do tipo de retorno do método de classificação.
"""

from abc import ABC, abstractmethod

from pipelines.rj_smtr__triagem_recursos.helpers.schemas import EsquemaDecisao


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
