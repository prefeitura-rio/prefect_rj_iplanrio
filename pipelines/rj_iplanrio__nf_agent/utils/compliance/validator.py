"""
Compliance Validator Module

This module contains the main compliance validation logic:
- compute_classification(): Determines if an NF is "OK", "Suspect", or "Not Analyzable"
- ComplianceValidator: Main validator class for validating extracted NFs against expected NFs
- validate_against_expected(): Convenience function for quick validation

The ``ComplianceValidator`` class is split into cohesive mixins to keep this
module tidy while preserving the public API.
"""

from pathlib import Path

from iplanrio_agent_toolkit.rules import RuleEngine

from .core import _LEGACY_FALLBACK_RESULT, ComplianceValidatorCoreMixin
from .matching import ComplianceValidatorMatchingMixin
from .report import ComplianceValidatorReportMixin
from .rules import DEFAULT_RULES
from .validate import ComplianceValidatorValidateMixin
from .validation_context import ValidationContext


def compute_classification(
    nf_found: bool,
    valor_pago: float | None = None,
    valor_documento: float | None = None,
    valor_extracted: float | None = None,
    tipo_documento: str | None = None,
    page_categories: list[str] | None = None,
    date_valid: bool | None = None,
    is_duplicate: bool | None = None,
) -> str:
    """
    [DEPRECATED] Use ComplianceValidator._classify or RuleEngine.evaluate instead.

    This function is kept for backward compatibility and now delegates to the rule engine.

    Compute classification: "OK", "Suspect", or "Not Analyzable"

    :param nf_found: Whether NF was found by extractor.
    :param valor_pago: Amount paid (from database).
    :param valor_documento: Expected total value (from database).
    :param valor_extracted: Extracted total value (from model).
    :param tipo_documento: Document type extracted by model.
    :param page_categories: List of page categories from classifier (optional).
    :param date_valid: Whether NF issue date is valid against company start date (optional).
    :param is_duplicate: Whether this NF appears in multiple different PDFs (optional).
    :returns: Classification: "OK", "Suspect", or "Not Analyzable".
    """
    # Create temporary rule engine with default rules
    engine = RuleEngine(DEFAULT_RULES)

    # Build context
    context = ValidationContext(
        nf_found=nf_found,
        valor_pago=valor_pago,
        valor_documento=valor_documento,
        valor_extracted=valor_extracted,
        tipo_documento=tipo_documento,
        page_categories=page_categories,
        date_valid=date_valid,
        is_duplicate=is_duplicate,
    )

    # Evaluate rules
    result = engine.evaluate(context, fallback=_LEGACY_FALLBACK_RESULT)

    return result.classification


class ComplianceValidator(
    ComplianceValidatorCoreMixin,
    ComplianceValidatorMatchingMixin,
    ComplianceValidatorValidateMixin,
    ComplianceValidatorReportMixin,
):
    """
    Validates extraction results against expected NFs.

    This is designed to be integrated as the final step in the extraction pipeline.
    """

    @staticmethod
    def load_expected_nfs_from_excel(excel_path: Path) -> list[dict]:
        """
        Load expected NFs from validation Excel file.

        :param excel_path: Path to validation Excel file with NF_Details sheet.
        :returns: List of expected NF dicts.
        :raises KeyError: If the sheet ``NF_Details`` is not found in the workbook.
        :raises ValueError: If required columns are missing from the sheet header.
        """
        from openpyxl import load_workbook

        wb = load_workbook(excel_path, read_only=True, data_only=True)
        if "NF_Details" not in wb.sheetnames:
            raise KeyError(f"Sheet 'NF_Details' not found in '{excel_path}'. Available sheets: {wb.sheetnames}")
        ws = wb["NF_Details"]

        rows = ws.iter_rows(values_only=True)
        header = next(rows)
        col_idx = {name: i for i, name in enumerate(header) if name is not None}

        required_cols = {"PDF_name", "CNPJ", "Numero_NF", "Valor_Total"}
        missing = required_cols - col_idx.keys()
        if missing:
            raise ValueError(f"Required columns missing from 'NF_Details' sheet: {sorted(missing)}")

        page_col = col_idx.get("NF_Page")
        expected_nfs = []
        for row in rows:
            expected_nfs.append(
                {
                    "pdf_name": row[col_idx["PDF_name"]],
                    "cnpj": row[col_idx["CNPJ"]],
                    "numero_nf": row[col_idx["Numero_NF"]],
                    "valor_total": row[col_idx["Valor_Total"]],
                    "page": row[page_col] if page_col is not None else "Unknown",
                }
            )

        wb.close()
        return expected_nfs


# Convenience function for quick validation
def validate_against_expected(
    pdf_name: str,
    extracted_nfs: list[dict],
    expected_nfs: list[dict],
    page_categories: list[str] | None = None,
) -> dict:
    """
    Quick validation of extraction results against expected NFs.

    :param pdf_name: Name of PDF file.
    :param extracted_nfs: List of extracted NF dicts.
    :param expected_nfs: List of expected NF dicts (all PDFs).
    :param page_categories: List of page categories from classifier (optional).
    :returns: Validation result dict with classification
        (OK/Suspect/Not Analyzable).
    """
    validator = ComplianceValidator(expected_nfs)
    return validator.validate_extraction(pdf_name, extracted_nfs, page_categories)
