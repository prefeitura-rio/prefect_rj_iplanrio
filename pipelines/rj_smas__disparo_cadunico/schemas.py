# -*- coding: utf-8 -*-
"""
Schemas de validação para payload do pipeline SMAS Disparo CADUNICO
Define estruturas de dados padronizadas com validação rigorosa usando Pydantic
"""

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, validator


class DestinationInput(BaseModel):
    """
    Schema para validação de dados de entrada (origem BigQuery)

    Campos obrigatórios:
    - to: Número de telefone (formato validado em etapa anterior)
    - externalId: Identificador externo único e não vazio

    Campos opcionais:
    - vars: Dicionário flexível com variáveis para o template HSM
    """

    to: str = Field(..., description="Número de telefone (formato validado em etapa anterior)")
    externalId: str = Field(..., min_length=1, description="Identificador externo obrigatório")
    vars: Optional[Dict[str, Any]] = Field(default=None, description="Variáveis opcionais para template HSM")

    @validator("to")
    def validate_phone(cls, v):
        """
        Valida que o telefone seja uma string não vazia
        Números são filtrados em etapa anterior

        Args:
            v: String do telefone a ser validada

        Returns:
            String do telefone validada

        Raises:
            ValueError: Se não for string ou estiver vazio
        """
        if not isinstance(v, str):
            raise ValueError("Telefone deve ser uma string")

        if not v.strip():
            raise ValueError("Telefone não pode ser vazio")

        return v.strip()

    @validator("externalId")
    def validate_external_id(cls, v):
        """
        Valida que externalId não seja vazio ou apenas espaços

        Args:
            v: String do externalId a ser validada

        Returns:
            String do externalId validada

        Raises:
            ValueError: Se for vazio ou apenas espaços
        """
        if not isinstance(v, str):
            raise ValueError("externalId deve ser uma string")

        if not v.strip():
            raise ValueError("externalId não pode ser vazio ou apenas espaços")

        return v.strip()


class DispatchPayload(BaseModel):
    """
    Schema para validação do payload enviado para WeTalkie API

    Campos obrigatórios:
    - campaignName: Nome da campanha não vazio
    - costCenterId: ID do centro de custo (número positivo)
    - destinations: Lista de destinatários validados
    """

    campaignName: str = Field(..., min_length=1, description="Nome da campanha")
    costCenterId: int = Field(..., gt=0, description="ID do centro de custo (deve ser positivo)")
    destinations: List[DestinationInput] = Field(..., description="Lista de destinatários validados")

    @validator("campaignName")
    def validate_campaign_name(cls, v):
        """
        Valida que o nome da campanha não seja apenas espaços

        Args:
            v: String do nome da campanha

        Returns:
            String do nome da campanha validada

        Raises:
            ValueError: Se for vazio ou apenas espaços
        """
        if not v.strip():
            raise ValueError("Nome da campanha não pode ser vazio ou apenas espaços")

        return v.strip()

    @validator("destinations")
    def validate_destinations_not_empty(cls, v):
        """
        Valida que a lista de destinatários não esteja vazia

        Args:
            v: Lista de destinatários

        Returns:
            Lista de destinatários validada

        Raises:
            ValueError: Se a lista estiver vazia
        """
        if not v:
            raise ValueError("Lista de destinatários não pode estar vazia")

        return v


class DispatchRecord(BaseModel):
    """
    Schema para registros salvos no BigQuery após o dispatch

    Representa um registro individual de disparo para auditoria
    """

    id_hsm: int = Field(..., description="ID do template HSM utilizado")
    dispatch_date: str = Field(..., description="Data e hora do disparo")
    campaignName: str = Field(..., description="Nome da campanha")
    costCenterId: int = Field(..., description="ID do centro de custo")
    to: str = Field(..., description="Número de telefone do destinatário")
    externalId: str = Field(..., description="Identificador externo do destinatário")
    vars: Optional[Dict[str, Any]] = Field(default=None, description="Variáveis utilizadas no template")


class ValidationStats(BaseModel):
    """
    Schema para estatísticas de validação

    Utilizado para tracking e logs de qualidade dos dados
    """

    total_input: int = Field(..., description="Total de registros de entrada")
    valid_records: int = Field(..., description="Registros válidos após validação")
    invalid_records: int = Field(..., description="Registros inválidos rejeitados")
    validation_errors: List[str] = Field(default_factory=list, description="Lista de erros encontrados")

    @property
    def success_rate(self) -> float:
        """
        Calcula taxa de sucesso da validação

        Returns:
            Float representando a porcentagem de registros válidos
        """
        if self.total_input == 0:
            return 0.0
        return (self.valid_records / self.total_input) * 100
