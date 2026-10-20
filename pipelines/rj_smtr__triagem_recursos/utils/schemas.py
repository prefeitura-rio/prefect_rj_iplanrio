"""
Definição dos Schemas Formais de Decisão e Auditoria Estruturada (Pydantic).

Estrutura os sinais de sistema extraídos dos espelhos/Nada Consta (CITRAN e Prescrição)
juntamente com os campos de deliberação prévia (CoT) e classificação final.
"""

from typing import Optional
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
    ENCAMINHAR_GIT = "encaminhar_git"
    ABRIR_PROCESSO_CITRAN = "abrir_processo_citran"
    ABRIR_PROCESSO = "abrir_processo"
    ANALISE_MANUAL = "analise_manual"
    PUBLICAR_INTEMPESTIVO = "publicar_intempestivo"


class AuditoriaSistema(BaseModel):
    """Extração seletiva de dados do espelho com impacto em regras de decisão."""
    prefixo_auto: Optional[str] = Field(
        default="NAO_IDENTIFICADO",
        description="Prefixo do auto de infração (ex.: RN, RA, RD, V, C, numérico ou NAO_IDENTIFICADO)."
    )
    codigo_status: Optional[int] = Field(
        default=None,
        description="Código numérico do status do auto no espelho SMTR (ex.: 30, 36, 71, 89)."
    )
    fase_descricao: Optional[str] = Field(
        default=None,
        description="Texto literal da fase processual no espelho (ex.: AUTUACAO, PENALIDADE, CANCELADO)."
    )
    data_expedicao_penalidade: Optional[str] = Field(
        default="NAO_IDENTIFICADO",
        description="Data de expedição da penalidade, VALOR_NULO_OU_TRACO se em branco/traços, ou NAO_IDENTIFICADO."
    )
    data_fato_infracao: Optional[str] = Field(
        default="NAO_IDENTIFICADO",
        description="Data do cometimento da infração no formato DD/MM/AAAA."
    )
    possui_espelho_nada_consta: bool = Field(
        default=True,
        description="Indica se o documento espelho/Nada Consta foi anexado aos autos."
    )


class EsquemaDecisao(BaseModel):
    """Schema estruturado estritamente alinhado às regras de triagem da JARI."""
    auditoria_sistema: AuditoriaSistema
    justificativa_analise_previa: str = Field(
        ...,
        description="Raciocínio analítico prévio estruturado em 2 a 3 frases (Fase 1 + Fase 2 + Motivação)."
    )
    frase_justificativa: str = Field(
        default="",
        description="Transcrição literal exata (ipsis litteris) do trecho ou dado. Se NENHUMA_MENCAO, string vazia ''."
    )
    pagina_evidencia: int = Field(
        default=0,
        ge=0,
        description="Número da página onde a evidência foi localizada. Se NENHUMA_MENCAO, preencher com 0."
    )
    is_manuscrito: bool = Field(
        default=False,
        description="True se o trecho foi escrito à mão (caneta/lápis); False se impresso/digitalizado de máquina."
    )
    tipo_citacao: TipoCitacao = Field(
        ...,
        description="Modalidade do trecho: DADO_ESTRUTURADO, FATO_DECLARADO, ALEGACAO_HIPOTETICA ou NENHUMA_MENCAO."
    )
    hipotese_legal: HipoteseLegal = Field(
        ...,
        description="Identificador único da hipótese legal cabível."
    )
    encaminhamento: Encaminhamento = Field(
        ...,
        description="Ação processual final a ser adotada."
    )