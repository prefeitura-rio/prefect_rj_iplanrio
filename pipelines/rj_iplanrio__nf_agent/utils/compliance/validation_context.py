"""
Validation Context

Context object that encapsulates all information needed for compliance rule evaluation.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class ValidationContext:
    """Context object containing all data needed for validation rules."""

    # Core flags
    nf_found: bool

    # Values
    valor_pago: float | None = None
    valor_documento: float | None = None
    valor_extracted: float | None = None

    # Document info
    tipo_documento: str | None = None
    page_categories: list[str] | None = None

    # Validation flags
    date_valid: bool | None = None
    is_duplicate: bool | None = None

    # Date fields for comparison
    data_emissao_expected: str | None = None  # From declaration
    data_emissao_extracted: str | None = None  # From PDF extraction

    # Service date vs company opening date validation
    data_servico: str | None = None  # Service provision date from PDF
    cnpj_data_abertura: str | None = None  # Company opening date from BigQuery

    # Helper properties
    @property
    def has_fatura_locacao(self) -> bool:
        """Check if any page category contains 'Fatura de Locação'."""
        if not self.page_categories:
            return False
        return any(cat and ("locação" in cat.lower() or "locacao" in cat.lower()) for cat in self.page_categories)
