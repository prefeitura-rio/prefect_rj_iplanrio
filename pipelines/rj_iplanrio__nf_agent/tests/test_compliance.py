"""Tests for the compliance rule engine (``utils/compliance/``).

This is already pure/testable logic (per the task brief): ``compute_classification``
delegates to a real ``RuleEngine`` + ``ValidationContext``, individual ``Rule``
subclasses are pure functions of a context object, and ``ComplianceValidator``
(with ``use_bigquery_deduplication=False``) only needs a real BigQuery call
mocked out for ``get_company_start_date`` (see ``conftest.py::no_bigquery_start_date``
— everything else is local dict/normalization logic).
"""

from __future__ import annotations

import pytest

from pipelines.rj_iplanrio__nf_agent.utils.compliance.rules.duplicate_nf import DuplicateNFRule
from pipelines.rj_iplanrio__nf_agent.utils.compliance.rules.missing_nf import MissingNFRule
from pipelines.rj_iplanrio__nf_agent.utils.compliance.rules.overpayment import OverpaymentRule
from pipelines.rj_iplanrio__nf_agent.utils.compliance.rules.unmapped_document_type import (
    UnmappedDocumentTypeRule,
)
from pipelines.rj_iplanrio__nf_agent.utils.compliance.validation_context import ValidationContext
from pipelines.rj_iplanrio__nf_agent.utils.compliance.validator import (
    ComplianceValidator,
    compute_classification,
)


class TestComputeClassification:
    def test_nf_not_found_is_suspect(self):
        assert compute_classification(nf_found=False) == "Suspect"

    def test_nf_found_with_matching_values_is_ok(self):
        result = compute_classification(
            nf_found=True,
            valor_pago=100.0,
            valor_documento=100.0,
            valor_extracted=100.0,
            tipo_documento="NFS-e",
        )
        assert result == "OK"

    def test_overpayment_is_suspect(self):
        result = compute_classification(
            nf_found=True,
            valor_pago=150.0,
            valor_extracted=100.0,
            tipo_documento="NFS-e",
        )
        assert result == "Suspect"

    def test_duplicate_is_suspect(self):
        result = compute_classification(nf_found=True, tipo_documento="NFS-e", is_duplicate=True)
        assert result == "Suspect"


class TestMissingNfRule:
    def test_applies_when_not_found(self):
        rule = MissingNFRule()
        result = rule.evaluate(ValidationContext(nf_found=False))
        assert result.applies is True
        assert result.classification == "Suspect"
        assert result.stop_evaluation is True

    def test_does_not_apply_when_found(self):
        rule = MissingNFRule()
        result = rule.evaluate(ValidationContext(nf_found=True))
        assert result.applies is False


class TestOverpaymentRule:
    def test_applies_when_paid_exceeds_extracted_beyond_tolerance(self):
        rule = OverpaymentRule()
        ctx = ValidationContext(nf_found=True, valor_pago=110.0, valor_extracted=100.0)
        result = rule.evaluate(ctx)
        assert result.applies is True
        assert result.classification == "Suspect"

    def test_does_not_apply_within_tolerance(self):
        rule = OverpaymentRule()
        ctx = ValidationContext(nf_found=True, valor_pago=100.005, valor_extracted=100.0)
        result = rule.evaluate(ctx)
        assert result.applies is False

    def test_does_not_apply_when_values_missing(self):
        rule = OverpaymentRule()
        assert rule.evaluate(ValidationContext(nf_found=True)).applies is False

    def test_does_not_apply_when_nf_not_found(self):
        rule = OverpaymentRule()
        ctx = ValidationContext(nf_found=False, valor_pago=999.0, valor_extracted=1.0)
        assert rule.evaluate(ctx).applies is False


class TestDuplicateNfRule:
    def test_applies_when_flagged_duplicate(self):
        rule = DuplicateNFRule()
        ctx = ValidationContext(nf_found=True, is_duplicate=True)
        result = rule.evaluate(ctx)
        assert result.applies is True
        assert result.classification == "Suspect"

    def test_does_not_apply_when_not_duplicate(self):
        rule = DuplicateNFRule()
        ctx = ValidationContext(nf_found=True, is_duplicate=False)
        assert rule.evaluate(ctx).applies is False


class TestUnmappedDocumentTypeRule:
    def test_applies_when_type_missing(self):
        rule = UnmappedDocumentTypeRule()
        ctx = ValidationContext(nf_found=True, tipo_documento=None)
        result = rule.evaluate(ctx)
        assert result.applies is True
        assert result.classification == "Not Analyzable"

    def test_applies_for_unmapped_keyword(self):
        rule = UnmappedDocumentTypeRule()
        ctx = ValidationContext(nf_found=True, tipo_documento="Nenhuma das Opções")
        assert rule.evaluate(ctx).applies is True

    def test_does_not_apply_for_known_type(self):
        rule = UnmappedDocumentTypeRule()
        ctx = ValidationContext(nf_found=True, tipo_documento="NFS-e")
        assert rule.evaluate(ctx).applies is False


class TestComplianceValidatorValidateExtraction:
    """End-to-end (still fully local) tests of ComplianceValidator, matching
    how ``process_pdf`` constructs it: ``use_bigquery_deduplication=False``,
    ``rules=[UnmappedDocumentTypeRule()]``.
    """

    def test_matched_nf_is_ok(self, no_bigquery_start_date):
        expected = [{"pdf_name": "a.pdf", "cnpj": "11.222.333/0001-44", "numero_nf": "123", "valor_total": 100.0}]
        extracted = [
            {
                "cnpj_emitente": "11.222.333/0001-44",
                "numero_nf": "123",
                "valor_total": 100.0,
                "tipo_documento": "NFS-e",
            }
        ]
        validator = ComplianceValidator(
            expected_nfs=expected, use_bigquery_deduplication=False, rules=[UnmappedDocumentTypeRule()]
        )
        result = validator.validate_extraction("a.pdf", extracted)

        assert result["status"] == "OK"
        assert result["summary"]["correctly_extracted"] == 1
        assert result["summary"]["missing"] == 0

    def test_missing_nf_is_problems(self, no_bigquery_start_date):
        expected = [{"pdf_name": "a.pdf", "cnpj": "11.222.333/0001-44", "numero_nf": "123", "valor_total": 100.0}]
        validator = ComplianceValidator(
            expected_nfs=expected, use_bigquery_deduplication=False, rules=[UnmappedDocumentTypeRule()]
        )
        result = validator.validate_extraction("a.pdf", extracted_nfs=[])

        assert result["status"] == "PROBLEMS"
        assert result["summary"]["missing"] == 1
        assert result["missing_nfs"][0]["numero_nf"] == "123"

    def test_no_expected_nfs_marks_extractions_suspicious(self, no_bigquery_start_date):
        validator = ComplianceValidator(expected_nfs=[], use_bigquery_deduplication=False)
        extracted = [{"cnpj_emitente": "11.222.333/0001-44", "numero_nf": "999", "valor_total": 10.0}]

        result = validator.validate_extraction("unknown.pdf", extracted)

        assert result["status"] == "WARNINGS"
        assert result["suspicious_extractions"] == extracted


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
