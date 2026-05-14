# -*- coding: utf-8 -*-
"""
Constantes e configurações para a pipeline da Força Municipal (Guarda Municipal RJ).
Sistema: HxGN OnCall - Gestão de Ocorrências e Unidades
API: CIVITAS/CORIO
"""

from typing import Dict


class EndpointConfig:
    """Configuração de endpoints da API CIVITAS/CORIO para Força Municipal."""

    # Mapeamento de table_id para endpoint da API
    ENDPOINTS: Dict[str, str] = {  # noqa: RUF012
        # === UNIDADES / VIATURAS ===
        "unidades_ativas": "/unidades/ativas",
        "unidades_historico": "/unidades/historico",
        # "unit_positions": "/unit/positions",

        # === OCORRÊNCIAS / EVENTOS ===
        "ocorrencias_ativas": "/ocorrencias/ativas",
        "ocorrencias_historico": "/ocorrencias/historico",
        "ocorrencias_ativas_v2": "/ocorrencias/ativas/v2",

        # === QMD - Quadro de Movimentação Diária ===
        "qmd": "/qmd",
        "qmd_ativos": "/qmd/ativos",
        "qmd_servicos": "/qmd/servicos",
        "qmd_missoes": "/qmd/missoes",
        "qmd_plano": "/qmd/plano",
    }

    # Descrições dos endpoints
    DESCRIPTIONS: Dict[str, str] = {  # noqa: RUF012
        # Unidades
        "unidades_ativas": "Viaturas logadas no sistema (status atual)",
        "unidades_historico": "Histórico completo de todas as viaturas",
        # "unit_positions": "Posições geográficas atualizadas das unidades",

        # Ocorrências
        "ocorrencias_ativas": "Ocorrências em curso do sistema",
        "ocorrencias_historico": "Histórico completo de todas as ocorrências",
        "ocorrencias_ativas_v2": "Ocorrências ativas (versão 2 com campos adicionais)",

        # QMD
        "qmd": "Todos os registros do QMD",
        "qmd_ativos": "Apenas registros ativos do QMD",
        "qmd_servicos": "Serviços cadastrados no QMD",
        "qmd_missoes": "Missões planejadas/em andamento",
        "qmd_plano": "Planos operacionais cadastrados",
    }

    # Categorias dos endpoints
    CATEGORIES: Dict[str, str] = {  # noqa: RUF012
        "unidades_ativas": "Unidades/Viaturas",
        "unidades_historico": "Unidades/Viaturas",
        # "unit_positions": "Unidades/Viaturas",
        "ocorrencias_ativas": "Ocorrências/Eventos",
        "ocorrencias_historico": "Ocorrências/Eventos",
        "ocorrencias_ativas_v2": "Ocorrências/Eventos",
        "qmd": "QMD",
        "qmd_ativos": "QMD",
        "qmd_servicos": "QMD",
        "qmd_missoes": "QMD",
        "qmd_plano": "QMD",
    }

    @classmethod
    def get_endpoint(cls, table_id: str) -> str:
        """
        Obtém o endpoint da API correspondente ao table_id.

        Args:
            table_id: ID da tabela (ex: "unidades_ativas")

        Returns:
            Endpoint da API (ex: "/unidades/ativas")

        Raises:
            ValueError: Se o table_id não estiver mapeado
        """
        if table_id not in cls.ENDPOINTS:
            available = ", ".join(cls.ENDPOINTS.keys())
            raise ValueError(
                f"table_id '{table_id}' não encontrado. "
                f"Disponíveis: {available}"
            )
        return cls.ENDPOINTS[table_id]

    @classmethod
    def get_description(cls, table_id: str) -> str:
        """
        Obtém a descrição do endpoint.

        Args:
            table_id: ID da tabela

        Returns:
            Descrição do endpoint
        """
        return cls.DESCRIPTIONS.get(table_id, "")

    @classmethod
    def get_category(cls, table_id: str) -> str:
        """
        Obtém a categoria do endpoint.

        Args:
            table_id: ID da tabela

        Returns:
            Categoria do endpoint
        """
        return cls.CATEGORIES.get(table_id, "")

    @classmethod
    def list_all(cls) -> Dict[str, Dict[str, str]]:
        """
        Lista todos os endpoints disponíveis com suas informações.

        Returns:
            Dicionário com table_id como chave e informações do endpoint
        """
        return {
            table_id: {
                "endpoint": endpoint,
                "description": cls.DESCRIPTIONS.get(table_id, ""),
                "category": cls.CATEGORIES.get(table_id, ""),
            }
            for table_id, endpoint in cls.ENDPOINTS.items()
        }

    @classmethod
    def list_by_category(cls, category: str) -> Dict[str, str]:
        """
        Lista endpoints de uma categoria específica.

        Args:
            category: Nome da categoria

        Returns:
            Dicionário com table_id e endpoint filtrados por categoria
        """
        return {
            table_id: endpoint
            for table_id, endpoint in cls.ENDPOINTS.items()
            if cls.CATEGORIES.get(table_id) == category
        }


# Configurações da API
API_CONFIG = {
    "base_url": "",
    "username": "",
    "password": "",
    "proxy": "",
    "verify_ssl": "",
    "timeout": "",
}

# Configurações de paginação padrão
PAGINATION_CONFIG = {
    "paginated": True,
    "page_size": 100,
}
