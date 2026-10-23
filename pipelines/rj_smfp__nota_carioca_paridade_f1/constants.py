"""Constantes do flow de paridade F1."""

from pipelines.rj_smfp__nota_carioca_bq_to_oracle.constants import DBT_DEPLOYMENT, DBT_POLL_SECONDS

__all__ = ["DBT_DEPLOYMENT", "DBT_POLL_SECONDS", "REPORT_ARTIFACT_KEY", "RESUMO_TABLE"]

RESUMO_TABLE = "rj-iplanrio-dia.nota_carioca_v2.paridade_nc2__resumo"
"""Tabela de resumo gravada pelo ``dbt build`` de ``tag:paridade_nc2`` (contrato definido pela trilha do dbt)."""

REPORT_ARTIFACT_KEY = "paridade-f1-relatorio"
"""Chave do artefato Markdown (com o relatório JSON) publicado pelo flow."""
