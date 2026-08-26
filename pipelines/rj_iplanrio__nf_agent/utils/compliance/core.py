"""Core classification and expected-NF lookup for ``ComplianceValidator``."""

import logging
from pathlib import Path
from typing import TYPE_CHECKING

from iplanrio_agent_toolkit.rules import Rule, RuleEngine, RuleResult

from .rules import DEFAULT_RULES
from .utils import normalize_cnpj, normalize_number, normalize_value
from .validation_context import ValidationContext

if TYPE_CHECKING:
    from .validator import ComplianceValidator

logger = logging.getLogger(__name__)

# Preserves the exact legacy fallback (pre-toolkit RuleEngine always returned this
# when no rule stopped evaluation). Should be unreachable in practice since
# DEFAULT_RULES always includes DefaultPassRule at priority=100.
_LEGACY_FALLBACK_RESULT = RuleResult(
    applies=True,
    classification="OK",
    stop_evaluation=True,
    reason="No validation issues found (fallback)",
    rule_name="Fallback",
)


def initialize(
    validator: "ComplianceValidator",
    expected_nfs: list[dict] = None,
    use_bigquery_deduplication: bool = True,
    service_account_path: Path | None = None,
    rules: list[Rule[ValidationContext]] | None = None,
    min_match_score: int = 2,
) -> None:
    """
    Initialize ``validator`` with expected NFs to search for.

    :param validator: The ``ComplianceValidator`` instance being constructed.
    :param expected_nfs: List of expected NF dicts with keys:
        - pdf_name: Name of PDF file
        - cnpj: Expected CNPJ (any format)
        - numero_nf: Expected NF number (any format)
        - valor_total: Expected total value (any format)
        - page: Expected page number (optional, for reporting)
    :param use_bigquery_deduplication: If True, query BigQuery for
        deduplication lookup. If False, use only expected_nfs (for testing).
    :param service_account_path: Optional path to BigQuery credentials.
    :param rules: Optional custom rule list (defaults to DEFAULT_RULES).
    :param min_match_score: Minimum number of fields (CNPJ + número +
        data_emissão) that must match for a declaration to be considered
        found. 2 = allow 2/3 match with fallback (default, legacy
        behaviour). 3 = require all 3 fields to match (strict / no fallback).
    """
    validator.expected_nfs = expected_nfs or []
    validator.use_bigquery_deduplication = use_bigquery_deduplication
    validator.service_account_path = service_account_path
    validator.min_match_score = min_match_score

    # Initialize rule engine
    validator.rule_engine = RuleEngine(rules or DEFAULT_RULES)

    # Build normalized lookup for fast matching
    _build_expected_lookup(validator)


def _build_expected_lookup(validator: "ComplianceValidator") -> None:
    """Build normalized lookup structure for expected NFs on ``validator``."""
    validator.expected_by_pdf = {}
    validator.pdf_data_envio = {}  # Maps pdf_name -> data_envio for duplicate detection

    # Build deduplication lookup from BigQuery OR from expected_nfs
    if validator.use_bigquery_deduplication:
        # Query BigQuery for complete deduplication lookup
        from ..run_poc.bigquery_loader import (
            get_deduplication_lookup_from_bigquery,
        )

        try:
            logger.info("Loading deduplication lookup from BigQuery...")
            validator.deduplication_lookup = get_deduplication_lookup_from_bigquery(
                service_account_path=validator.service_account_path
            )
            logger.info(
                "Loaded %d unique (CNPJ, Numero) combinations",
                len(validator.deduplication_lookup),
            )

            # Count duplicates
            duplicates = sum(1 for pdfs in validator.deduplication_lookup.values() if len(pdfs) > 1)
            logger.info("Found %d combinations appearing in multiple PDFs", duplicates)
        except Exception as e:
            logger.warning("Failed to load BigQuery deduplication lookup: %s", e)
            logger.warning("Falling back to local expected_nfs for deduplication")
            validator.deduplication_lookup = {}
            validator.use_bigquery_deduplication = False
    else:
        # Fallback: Build from local expected_nfs only (for testing)
        validator.deduplication_lookup = {}

    # Build expected_by_pdf and local deduplication (if BigQuery failed)
    for nf in validator.expected_nfs:
        pdf_name = nf["pdf_name"]

        if pdf_name not in validator.expected_by_pdf:
            validator.expected_by_pdf[pdf_name] = []

        # Store data_envio for this PDF (for duplicate detection)
        if "data_envio" in nf and pdf_name not in validator.pdf_data_envio:
            validator.pdf_data_envio[pdf_name] = nf["data_envio"]

        cnpj_norm = normalize_cnpj(nf["cnpj"])
        numero_norm = normalize_number(nf["numero_nf"])
        valor_norm = normalize_value(nf["valor_total"])

        validator.expected_by_pdf[pdf_name].append(
            {
                "original": nf,
                "cnpj_norm": cnpj_norm,
                "numero_norm": numero_norm,
                "valor_norm": valor_norm,
                "cod_organizacao": nf.get("cod_organizacao"),  # For duplicate detection
                "cod_unidade": nf.get("cod_unidade"),  # For duplicate detection
                "id_documento": nf.get("id_documento"),  # For duplicate detection tie-breaking
                "matched": False,  # Track if this expected NF was found
            }
        )

        # If not using BigQuery, build local deduplication from expected_nfs
        if not validator.use_bigquery_deduplication:
            # 4-field dedup key: exact (cnpj, numero, org, unit) combination must repeat
            cod_org = nf.get("cod_organizacao", "")
            cod_unit = nf.get("cod_unidade", "")
            dedup_key = (cnpj_norm, numero_norm, cod_org, cod_unit)

            if dedup_key not in validator.deduplication_lookup:
                validator.deduplication_lookup[dedup_key] = []

            # Store fields matching BigQuery structure
            entry = {
                "pdf_name": pdf_name,
                "data_envio": nf.get("data_envio"),
                "id_documento": nf.get("id_documento"),
                "cod_organizacao": cod_org,
                "cod_unidade": cod_unit,
            }

            # Only add if not already in list
            if not any(e["pdf_name"] == pdf_name for e in validator.deduplication_lookup[dedup_key]):
                validator.deduplication_lookup[dedup_key].append(entry)


def classify(
    validator: "ComplianceValidator",
    nf_found: bool,
    valor_pago: float | None = None,
    valor_documento: float | None = None,
    valor_extracted: float | None = None,
    tipo_documento: str | None = None,
    page_categories: list[str] | None = None,
    date_valid: bool | None = None,
    is_duplicate: bool | None = None,
    data_emissao_expected: str | None = None,
    data_emissao_extracted: str | None = None,
    data_servico: str | None = None,
    cnpj_data_abertura: str | None = None,
) -> RuleResult:
    """
    Classify NF using ``validator``'s rule engine.

    This replaces the old compute_classification function.

    :param validator: The ``ComplianceValidator`` instance (supplies the rule engine).
    :param nf_found: Whether NF was found by extractor.
    :param valor_pago: Amount paid (from database).
    :param valor_documento: Expected total value (from database).
    :param valor_extracted: Extracted total value (from model).
    :param tipo_documento: Document type extracted by model.
    :param page_categories: List of page categories from classifier (optional).
    :param date_valid: Whether NF issue date is valid against company start date (optional).
    :param is_duplicate: Whether this NF appears in multiple different PDFs (optional).
    :param data_emissao_expected: Expected emission date from declaration (optional).
    :param data_emissao_extracted: Extracted emission date from PDF (optional).
    :param data_servico: Service provision date from PDF (optional).
    :param cnpj_data_abertura: Company opening date from BigQuery (optional).
    :returns: RuleResult with classification, rule_name, and reason.
    """
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
        data_emissao_expected=data_emissao_expected,
        data_emissao_extracted=data_emissao_extracted,
        data_servico=data_servico,
        cnpj_data_abertura=cnpj_data_abertura,
    )

    # Evaluate rules
    result = validator.rule_engine.evaluate(context, fallback=_LEGACY_FALLBACK_RESULT)

    return result  # Return full RuleResult with classification, rule_name, reason
