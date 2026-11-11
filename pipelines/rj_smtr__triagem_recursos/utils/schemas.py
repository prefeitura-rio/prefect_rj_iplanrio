"""Esquemas formais de dados, contratos de inferência e validação Pydantic.

Descrição Técnica & Arquitetura:
Define o vocabulário canônico do sistema e os contratos de dados compartilhados entre
a pipeline de triagem, a esteira de calibração estatística e o servidor MCP do Claude Desktop.
Centraliza as enumerações das 7 hipóteses legais de mérito da SMTR, os 5 tipos de
encaminhamento processual, a modalidade linguística das citações (fato declarado,
dado estruturado, alegação hipotética) e o modelo estruturado `EsquemaDecisao`,
que força saídas tipadas e determinísticas nas chamadas multimodais da LLM.

Fluxo de Dependências:
- Consumidores Diretos:
  * `utils/router.py`: utiliza `EsquemaDecisao` para compor `DecisaoProcessualFinal` e rotear rotas de execução.
  * `utils/triage.py`: valida e tipa a resposta bruta das chamadas N=1 e N=2 da LLM.
  * `utils/guardrails.py`: consome os Enums para aplicar salvaguardas epistêmicas e conversões de mérito.
  * `utils/feature_extractor.py`: extrai métricas de citação a partir dos campos de evidência do schema.
  * `utils/providers/base.py`, `openai_client.py`, `google_client.py`: impõem este schema como saída estruturada (JSON Schema) nos provedores.
- Dependências Diretas:
  * Módulo folha de tipos canônicos (sem dependências internas de outros submódulos da pipeline).
"""

from enum import Enum

from pydantic import BaseModel, Field


class TipoCitacao(str, Enum):
    """Classificação formal da modalidade linguística ou técnica do trecho identificado."""

    FATO_DECLARADO = "FATO_DECLARADO"
    DADO_ESTRUTURADO = "DADO_ESTRUTURADO"
    ALEGACAO_HIPOTETICA = "ALEGACAO_HIPOTETICA"
    NENHUMA_MENCAO = "NENHUMA_MENCAO"


class HipoteseLegal(str, Enum):
    """Hipóteses formais de admissibilidade ou mérito esgotado."""

    CITRAN = "citran"
    ROUBO_FURTO_CLONAGEM = "roubo_furto_clonagem"
    ERRO_CAPTURA_IMAGEM = "erro_captura_imagem"
    VEICULO_ESPECIAL = "veiculo_especial"
    NAO_INDICACAO_CONDUTOR = "nao_indicacao_condutor"
    PRESCRICAO = "prescricao"
    HIPOTESE_ESGOTADA = "hipotese_esgotada"


class Encaminhamento(str, Enum):
    """Ações processuais finais para o recurso."""

    ENCAMINHAR_GIT = "encaminhar_git"
    ABRIR_PROCESSO_CITRAN = "abrir_processo_citran"
    ABRIR_PROCESSO = "abrir_processo"
    ANALISE_MANUAL = "analise_manual"
    PUBLICAR_INTEMPESTIVO = "publicar_intempestivo"


class AuditoriaSistema(BaseModel):
    """Extração seletiva de dados do espelho com impacto em regras de decisão."""

    prefixo_auto: str | None = Field(
        default="NAO_IDENTIFICADO",
        description="Prefixo do auto de infração (ex.: RN, RA, RD, V, C, numérico ou NAO_IDENTIFICADO).",
    )
    codigo_status: int | None = Field(
        default=None,
        description="Código numérico do status do auto no espelho SMTR (ex.: 30, 36, 71, 89).",
    )
    fase_descricao: str | None = Field(
        default=None,
        description="Texto literal da fase processual no espelho (ex.: AUTUACAO, PENALIDADE, CANCELADO).",
    )
    data_expedicao_penalidade: str | None = Field(
        default="NAO_IDENTIFICADO",
        description="Data de expedição da penalidade, VALOR_NULO_OU_TRACO se em branco/traços, ou NAO_IDENTIFICADO.",
    )
    data_fato_infracao: str | None = Field(
        default="NAO_IDENTIFICADO",
        description="Data do cometimento da infração no formato DD/MM/AAAA.",
    )
    possui_espelho_nada_consta: bool = Field(
        default=True,
        description="Indica se o documento espelho/Nada Consta foi anexado aos autos.",
    )


class EsquemaDecisao(BaseModel):
    """Schema estruturado estritamente alinhado às regras de triagem da JARI."""

    auditoria_sistema: AuditoriaSistema
    justificativa_analise_previa: str = Field(
        ...,
        description="Raciocínio analítico prévio estruturado em 2 a 3 frases (Fase 1 + Fase 2 + Motivação).",
    )
    frase_justificativa: str = Field(
        default="",
        description="Transcrição literal exata (ipsis litteris) do trecho ou dado. Se NENHUMA_MENCAO, string vazia ''.",
    )
    pagina_evidencia: int = Field(
        default=0,
        ge=0,
        description="Número da página onde a evidência foi localizada. Se NENHUMA_MENCAO, preencher com 0.",
    )
    is_manuscrito: bool = Field(
        default=False,
        description="True se o trecho foi escrito à mão (caneta/lápis); False se impresso/digitalizado de máquina.",
    )
    tipo_citacao: TipoCitacao = Field(
        ...,
        description="Modalidade do trecho: DADO_ESTRUTURADO, FATO_DECLARADO, ALEGACAO_HIPOTETICA ou NENHUMA_MENCAO.",
    )
    hipotese_legal: HipoteseLegal = Field(
        ...,
        description="Identificador único da hipótese legal cabível.",
    )
    encaminhamento: Encaminhamento = Field(
        ...,
        description="Ação processual final a ser adotada.",
    )
