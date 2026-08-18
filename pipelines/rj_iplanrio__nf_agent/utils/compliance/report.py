"""Human-friendly reports for ``ComplianceValidator``."""

import logging

logger = logging.getLogger(__name__)


class ComplianceValidatorReportMixin:
    """Text reports for validation and batch results."""

    def print_validation_report(self, validation: dict, verbose: bool = True):
        """
        Log a human-readable validation report for a single PDF.

        :param validation: Validation result dict.
        :param verbose: If True, show detailed lists of NFs.
        """
        pdf_name = validation.get('pdf_name', 'Unknown')
        status = validation['status']
        summary = validation['summary']

        status_symbol = {'OK': '[OK]', 'WARNINGS': '[WARNING]', 'PROBLEMS': '[PROBLEM]'}.get(status, '[?]')

        logger.info("%s %s - Status: %s", status_symbol, pdf_name, status)
        logger.info("  Expected: %d NFs", summary['total_expected'])
        logger.info("  Extracted: %d NFs", summary['total_extracted'])
        logger.info("  Correctly extracted: %d", summary['correctly_extracted'])

        if summary['missing'] > 0:
            logger.warning("  [!] Missing: %d NFs", summary['missing'])

        if summary['suspicious'] > 0:
            logger.warning("  [!] Suspicious: %d extractions", summary['suspicious'])

        if summary['normalization_issues'] > 0:
            logger.warning("  [!] Normalization issues: %d", summary['normalization_issues'])

        if verbose and status != 'OK':
            if validation['missing_nfs']:
                logger.info("  Missing NFs:")
                for nf in validation['missing_nfs']:
                    logger.info(
                        "    - CNPJ: %s, Numero: %s, Valor: %s",
                        nf['cnpj'], nf['numero_nf'], nf['valor_total'],
                    )

            if validation['suspicious_extractions']:
                logger.info("  Suspicious Extractions:")
                for item in validation['suspicious_extractions']:
                    ext = item['extracted']
                    logger.info(
                        "    - CNPJ: %s, Numero: %s | Reason: %s",
                        ext.get('cnpj_emitente', 'N/A'), ext.get('numero_nf', 'N/A'), item['reason'],
                    )

            if validation['normalization_issues']:
                logger.info("  Normalization Issues:")
                for item in validation['normalization_issues']:
                    exp = item['expected']
                    logger.info(
                        "    - Expected: %s | Extracted: %s | Issue: %s",
                        exp['numero_nf'], item['extracted_numero'], item['issue'],
                    )

    def print_batch_report(self, batch_validation: dict, group_by_nf: bool = True):
        """
        Log aggregate batch validation report.

        :param batch_validation: Batch validation result dict.
        :param group_by_nf: If True, group results by searched NF instead of by PDF.
        """
        summary = batch_validation['aggregate_summary']

        sep = "=" * 80
        logger.info(sep)
        logger.info("COMPLIANCE VALIDATION REPORT")
        logger.info(sep)
        logger.info("Total PDFs: %d", summary['total_pdfs'])
        logger.info("  [OK] Status OK: %d", summary['pdfs_ok'])
        logger.info("  [WARN] Warnings: %d", summary['pdfs_with_warnings'])
        logger.info("  [PROB] Problems: %d", summary['pdfs_with_problems'])
        logger.info("Total Expected NFs: %d", summary['total_expected_nfs'])
        logger.info("Total Extracted NFs: %d", summary['total_extracted_nfs'])
        logger.info("Correctly Extracted: %d", summary['correctly_extracted'])
        logger.info("Missing NFs: %d", summary['missing_nfs'])
        logger.info("Suspicious Extractions: %d", summary['suspicious_extractions'])
        logger.info("Normalization Issues: %d", summary['normalization_issues'])
        logger.info("Classification Breakdown:")
        logger.info("  [OK] OK: %d", summary.get('classification_ok', 0))
        logger.info("  [SUSPECT] Suspect: %d", summary.get('classification_suspect', 0))
        logger.info("  [N/A] Not Analyzable: %d", summary.get('classification_not_analyzable', 0))
        logger.info("Precision: %.2f%%", summary['precision'])
        logger.info("Recall: %.2f%%", summary['recall'])
        logger.info(sep)

        if group_by_nf:
            self._print_nf_centric_report(batch_validation)

    def _print_nf_centric_report(self, batch_validation: dict):
        """
        Print NF-centric view: show status of each searched NF.

        :param batch_validation: Batch validation result dict.
        """
        sep = "=" * 80
        logger.info(sep)
        logger.info("SEARCHED NFs - DETAILED STATUS")
        logger.info(sep)

        # Collect all NF statuses
        nf_statuses = []

        for validation in batch_validation['validations']:
            pdf_name = validation['pdf_name']

            for nf_data in validation['correctly_extracted']:
                expected = nf_data['expected']
                extracted = nf_data['extracted']
                nf_statuses.append({
                    'pdf_name': pdf_name,
                    'cnpj': expected['cnpj'],
                    'numero_nf': expected['numero_nf'],
                    'valor_total': expected['valor_total'],
                    'page': expected.get('page', 'Unknown'),
                    'status': 'FOUND',
                    'match_quality': nf_data['match_quality'],
                    'extracted_valor': extracted.get('valor_total', 0.0),
                    'classification': nf_data.get('classification', 'Unknown'),
                })

            for nf_data in validation['missing_nfs']:
                nf_statuses.append({
                    'pdf_name': pdf_name,
                    'cnpj': nf_data['cnpj'],
                    'numero_nf': nf_data['numero_nf'],
                    'valor_total': nf_data['valor_total'],
                    'page': nf_data.get('page', 'Unknown'),
                    'status': 'MISSING',
                    'match_quality': None,
                    'extracted_valor': None,
                    'classification': nf_data.get('classification', 'Unknown'),
                })

            for nf_data in validation['normalization_issues']:
                expected = nf_data['expected']
                nf_statuses.append({
                    'pdf_name': pdf_name,
                    'cnpj': expected['cnpj'],
                    'numero_nf': expected['numero_nf'],
                    'valor_total': expected['valor_total'],
                    'page': expected.get('page', 'Unknown'),
                    'status': 'NORM_ISSUE',
                    'match_quality': None,
                    'extracted_numero': nf_data['extracted_numero'],
                    'issue': nf_data['issue'],
                })

        status_order = {'FOUND': 0, 'NORM_ISSUE': 1, 'MISSING': 2}
        nf_statuses.sort(key=lambda x: (status_order.get(x['status'], 3), x['pdf_name'], x['cnpj']))

        found_count = 0
        missing_count = 0
        norm_issue_count = 0

        for nf in nf_statuses:
            if nf['status'] == 'FOUND':
                found_count += 1
                logger.info(
                    "[OK] FOUND | PDF: %s | CNPJ: %s | Numero: %s | "
                    "Valor esperado: %s | Valor extraído: %s | "
                    "Match: %s | Classificação: %s | Página: %s",
                    nf['pdf_name'], nf['cnpj'], nf['numero_nf'],
                    nf['valor_total'], nf['extracted_valor'],
                    nf['match_quality'], nf.get('classification', 'Unknown'), nf['page'],
                )

            elif nf['status'] == 'NORM_ISSUE':
                norm_issue_count += 1
                logger.warning(
                    "[WARN] NORMALIZATION ISSUE | PDF: %s | CNPJ: %s | "
                    "Número esperado: %s | Número extraído: %s | "
                    "Valor: %s | Problema: %s | Página: %s",
                    nf['pdf_name'], nf['cnpj'], nf['numero_nf'],
                    nf['extracted_numero'], nf['valor_total'], nf['issue'], nf['page'],
                )

            elif nf['status'] == 'MISSING':
                missing_count += 1
                logger.warning(
                    "[MISS] NOT FOUND | PDF: %s | CNPJ: %s | Numero: %s | "
                    "Valor: %s | Classificação: %s | Página: %s",
                    nf['pdf_name'], nf['cnpj'], nf['numero_nf'],
                    nf['valor_total'], nf.get('classification', 'Unknown'), nf['page'],
                )

        logger.info(sep)
        logger.info("SUSPICIOUS EXTRACTIONS (Not in expected list)")
        logger.info(sep)

        suspicious_count = 0
        for validation in batch_validation['validations']:
            pdf_name = validation['pdf_name']
            for suspicious in validation['suspicious_extractions']:
                suspicious_count += 1
                extracted = suspicious['extracted']
                logger.warning(
                    "[!] SUSPICIOUS - %s | CNPJ: %s | Numero: %s | Valor: %s | Reason: %s",
                    pdf_name,
                    extracted.get('cnpj_emitente', 'N/A'),
                    extracted.get('numero_nf', 'N/A'),
                    extracted.get('valor_total', 'N/A'),
                    suspicious['reason'],
                )

        if suspicious_count == 0:
            logger.info("No suspicious extractions found.")

        logger.info(sep)
        logger.info("SUMMARY BY SEARCHED NF")
        logger.info(sep)
        logger.info("Found: %d", found_count)
        logger.info("Missing: %d", missing_count)
        logger.info("Normalization Issues: %d", norm_issue_count)
        logger.info("Suspicious Extractions: %d", suspicious_count)
        logger.info(sep)
