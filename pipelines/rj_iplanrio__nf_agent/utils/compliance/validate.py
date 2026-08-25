"""Batch extraction validation for ``ComplianceValidator``."""

import logging

from .utils import normalize_cnpj

logger = logging.getLogger(__name__)


class ComplianceValidatorValidateMixin:
    """Validate extraction results against expected NFs."""

    def validate_extraction(
        self,
        pdf_name: str,
        extracted_nfs: list[dict],
        page_categories: list[str] | None = None,
    ) -> dict:
        """
        Validate extraction results for a single PDF against expected NFs.

        :param pdf_name: Name of PDF file.
        :param extracted_nfs: List of extracted NF dicts with keys:
            - cnpj_emitente: Extracted CNPJ
            - numero_nf: Extracted NF number
            - valor_total or valor_total_servico: Extracted value
            - tipo_documento: Document type (optional, for classification)
        :param page_categories: List of page categories from classifier
            (optional, for classification).
        :returns: Validation result dict with:
            - status: 'OK', 'WARNINGS', or 'PROBLEMS'
            - correctly_extracted: List of correctly matched NFs (with classification)
            - missing_nfs: List of expected NFs not found (with classification)
            - suspicious_extractions: List of extracted NFs not in expected list
            - normalization_issues: List of NFs with normalization mismatches
            - summary: Dict with counts (including classification breakdown)
        """
        # Get expected NFs for this PDF
        expected_list = self.expected_by_pdf.get(pdf_name, [])

        # TODO: move the composal of this object to a separate function
        # Build CNPJ → inicio_atividade_data cache from BigQuery
        cnpj_start_dates = {}

        # Get all unique CNPJs from extracted NFs
        unique_cnpjs = set(normalize_cnpj(nf.get("cnpj_emitente", "")) for nf in extracted_nfs)

        # Query BigQuery for each unique CNPJ
        from ..run_poc.bigquery_loader import (
            get_company_start_date,
        )

        for cnpj in unique_cnpjs:
            if cnpj:  # Skip empty CNPJs
                try:
                    start_date = get_company_start_date(cnpj)
                    cnpj_start_dates[cnpj] = start_date
                except Exception as e:
                    # Log error but continue (validation will skip if start date is missing)
                    logger.warning("Failed to query company start date for %s: %s", cnpj, e)

        # If no expected NFs for this PDF, all extractions are suspicious
        if not expected_list:
            return {
                "status": "WARNINGS" if len(extracted_nfs) > 0 else "OK",
                "correctly_extracted": [],
                "missing_nfs": [],
                "suspicious_extractions": extracted_nfs,
                "normalization_issues": [],
                "summary": {
                    "total_expected": 0,
                    "total_extracted": len(extracted_nfs),
                    "correctly_extracted": 0,
                    "missing": 0,
                    "suspicious": len(extracted_nfs),
                    "normalization_issues": 0,
                },
                "message": f"No expected NFs for {pdf_name} - all {len(extracted_nfs)} extractions are suspicious",
            }

        # NEW STRATEGY: Process each declaration independently (declaração-centric)
        logger.info(f"Processing {len(expected_list)} declarations for {pdf_name}")

        correctly_extracted = []
        missing_nfs = []

        for exp_nf in expected_list:
            # Store pdf_name in expected_nf for deduplication check
            exp_nf["pdf_name"] = pdf_name

            # Process this single declaration
            result = self._process_single_declaration(exp_nf, extracted_nfs, cnpj_start_dates, page_categories or [])

            # Categorize result
            if result.get("extracted"):
                # Match found
                correctly_extracted.append(result)
            else:
                # Missing NF
                missing_nfs.append(result["expected"])

        # Determine overall status
        has_problems = len(missing_nfs) > 0

        if has_problems:
            status = "PROBLEMS"
        else:
            status = "OK"

        # Count classifications
        classification_counts = {"OK": 0, "Suspect": 0, "Not Analyzable": 0, "Apontamento Leve": 0}

        # Count from correctly extracted
        for item in correctly_extracted:
            classification_counts[item.get("classification", "OK")] += 1

        # Count from missing
        for item in missing_nfs:
            classification_counts[item.get("classification", "Suspect")] += 1

        logger.info(
            f"Validation complete for {pdf_name}: {len(correctly_extracted)} matched, {len(missing_nfs)} missing"
        )

        return {
            "status": status,
            "correctly_extracted": correctly_extracted,
            "missing_nfs": missing_nfs,
            "suspicious_extractions": [],  # Not used in new strategy
            "normalization_issues": [],  # Not used in new strategy
            "summary": {
                "total_expected": len(expected_list),
                "total_extracted": len(extracted_nfs),
                "correctly_extracted": len(correctly_extracted),
                "missing": len(missing_nfs),
                "suspicious": 0,  # Not used in new strategy
                "normalization_issues": 0,  # Not used in new strategy
                # Classification breakdown
                "classification_ok": classification_counts["OK"],
                "classification_suspect": classification_counts["Suspect"],
                "classification_not_analyzable": classification_counts["Not Analyzable"],
            },
        }

    def validate_batch(self, extraction_results: list[dict]) -> dict:
        """
        Validate a batch of extraction results.

        :param extraction_results: List of dicts with:
            - pdf_name: Name of PDF
            - extracted_nfs: List of extracted NFs
        :returns: Batch validation result with aggregated statistics.
        """
        validations = []

        for result in extraction_results:
            pdf_name = result["pdf_name"]
            extracted_nfs = result.get("extracted_nfs", [])

            validation = self.validate_extraction(pdf_name, extracted_nfs)
            validation["pdf_name"] = pdf_name
            validations.append(validation)

        # Aggregate statistics
        total_expected = sum(v["summary"]["total_expected"] for v in validations)
        total_extracted = sum(v["summary"]["total_extracted"] for v in validations)
        total_correct = sum(v["summary"]["correctly_extracted"] for v in validations)
        total_missing = sum(v["summary"]["missing"] for v in validations)
        total_suspicious = sum(v["summary"]["suspicious"] for v in validations)
        total_norm_issues = sum(v["summary"]["normalization_issues"] for v in validations)

        # Aggregate classification counts
        total_ok = sum(v["summary"].get("classification_ok", 0) for v in validations)
        total_suspect = sum(v["summary"].get("classification_suspect", 0) for v in validations)
        total_not_analyzable = sum(v["summary"].get("classification_not_analyzable", 0) for v in validations)

        pdfs_with_problems = sum(1 for v in validations if v["status"] == "PROBLEMS")
        pdfs_with_warnings = sum(1 for v in validations if v["status"] == "WARNINGS")
        pdfs_ok = sum(1 for v in validations if v["status"] == "OK")

        # Calculate metrics
        precision = 100 * total_correct / total_extracted if total_extracted > 0 else 0
        recall = 100 * total_correct / total_expected if total_expected > 0 else 0

        return {
            "validations": validations,
            "aggregate_summary": {
                "total_pdfs": len(validations),
                "pdfs_with_problems": pdfs_with_problems,
                "pdfs_with_warnings": pdfs_with_warnings,
                "pdfs_ok": pdfs_ok,
                "total_expected_nfs": total_expected,
                "total_extracted_nfs": total_extracted,
                "correctly_extracted": total_correct,
                "missing_nfs": total_missing,
                "suspicious_extractions": total_suspicious,
                "normalization_issues": total_norm_issues,
                "precision": precision,
                "recall": recall,
                # Classification breakdown
                "classification_ok": total_ok,
                "classification_suspect": total_suspect,
                "classification_not_analyzable": total_not_analyzable,
            },
        }
