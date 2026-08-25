"""NF matching and per-declaration handling for ``ComplianceValidator``."""

import logging

from .utils import check_date_against_company_start, normalize_cnpj, normalize_number, normalize_value

logger = logging.getLogger(__name__)


class ComplianceValidatorMatchingMixin:
    """Duplicate detection, standard matches, and merge/missing handlers."""

    def _is_duplicate_nf(
        self,
        cnpj_norm: str,
        numero_norm: str,
        cod_organizacao: str,
        cod_unidade: str,
        pdf_name: str,
        id_documento: int | None = None,
        data_envio: str | None = None,
    ) -> bool:
        """
        Check if NF is a duplicate using 4-field logic.

        Duplicate Detection Rules (from TODO in duplicate_nf.py):
        1. Dedup key: (cnpj, numero_nf, cod_organizacao, cod_unidade) - exact 4-tuple must repeat
        2. Special case: If cod_organizacao == cod_unidade (sede/headquarters) → NOT duplicate
        3. Order by (data_envio, id_documento) - FIRST submission is original
        4. All submissions AFTER the first are duplicates

        :param cnpj_norm: Normalized CNPJ.
        :param numero_norm: Normalized numero_nf.
        :param cod_organizacao: Organization code.
        :param cod_unidade: Unit code.
        :param pdf_name: Current PDF name.
        :param id_documento: Document ID (tie-breaker when dates are equal).
        :param data_envio: Submission date.
        :returns: True if this NF is a duplicate (same 4-tuple submitted after
            the first), False if this is the first submission or is "sede"
            (headquarters).
        """
        # Special case: "sede" (headquarters) - never duplicate
        # When cod_organizacao == cod_unidade, it's a headquarters unit with special business rules
        if cod_organizacao and cod_unidade and cod_organizacao == cod_unidade:
            return False

        # Build 4-field dedup key: exact combination must repeat
        cod_org_norm = cod_organizacao if cod_organizacao else ""
        cod_unit_norm = cod_unidade if cod_unidade else ""
        dedup_key = (cnpj_norm, numero_norm, cod_org_norm, cod_unit_norm)

        pdf_list = self.deduplication_lookup.get(dedup_key, [])

        # If this exact 4-tuple appears in only 1 PDF or less → not a duplicate
        if len(pdf_list) <= 1:
            return False

        # Get current submission date
        if data_envio is None:
            data_envio = self.pdf_data_envio.get(pdf_name)

        if not data_envio:
            # No date info → fall back to simple count-based logic
            # If 4-tuple appears multiple times, mark all except one as duplicate
            return len(pdf_list) > 1

        # Check if any other record with same 4-tuple has EARLIER (data_envio, id_documento)
        from datetime import datetime

        for entry in pdf_list:
            other_pdf = entry["pdf_name"]
            other_date = entry.get("data_envio")
            other_id = entry.get("id_documento")

            # Skip if same PDF
            if other_pdf == pdf_name:
                continue

            # Skip if other date is missing
            if not other_date:
                continue

            # Compare (data_envio, id_documento) tuples
            try:
                # Parse dates for comparison
                if isinstance(data_envio, str):
                    current_date_obj = datetime.fromisoformat(data_envio.replace("/", "-"))
                else:
                    current_date_obj = data_envio

                if isinstance(other_date, str):
                    other_date_obj = datetime.fromisoformat(other_date.replace("/", "-"))
                else:
                    other_date_obj = other_date

                # Compare dates first
                if other_date_obj < current_date_obj:
                    return True  # Other is earlier → current is duplicate
                elif other_date_obj == current_date_obj:
                    # Dates are equal → use id_documento as tie-breaker
                    if id_documento is not None and other_id is not None:
                        if other_id < id_documento:
                            return True  # Other has lower ID → current is duplicate

            except Exception:
                # If date parsing fails, fall back to simple comparison
                try:
                    # Compare as strings/integers
                    if other_date < data_envio:
                        return True
                    elif other_date == data_envio and id_documento is not None and other_id is not None:
                        if other_id < id_documento:
                            return True
                except Exception:
                    pass

        # No earlier submission found → current is the first (original)
        return False

    def _find_standard_matches(
        self,
        cnpj_declarado: str,
        numero_declarado: str,
        data_declarada: str | None,
        extracted_nfs: list[dict],
        min_match_score: int = 2,
    ) -> list[dict]:
        """
        Find documents that match using 3-field scoring (CNPJ + número + data_emissão).

        Uses flexible matching where any 2 of 3 fields matching is sufficient.
        This handles cases where one field might have minor discrepancies.

        :param cnpj_declarado: Normalized CNPJ from declaration.
        :param numero_declarado: Normalized número from declaration.
        :param data_declarada: Data de emissão from declaration (optional).
        :param extracted_nfs: List of all extracted documents.
        :param min_match_score: Minimum number of fields that must match
            (default: 2).
        :returns: List of documents that match (annotated with _match_score).
        """
        from .utils import DocumentFields, match_score_3_fields

        matches = []

        for ext_nf in extracted_nfs:
            # Calculate match score using all 3 fields
            score = match_score_3_fields(
                expected=DocumentFields(
                    cnpj=cnpj_declarado,
                    numero=numero_declarado,
                    data=data_declarada,
                ),
                extracted=DocumentFields(
                    cnpj=ext_nf.get("cnpj_emitente", ""),
                    numero=ext_nf.get("numero_nf", ""),
                    data=ext_nf.get("data_emissao"),
                ),
            )

            # Only include if score meets minimum threshold
            if score >= min_match_score:
                # Annotate the match with score information
                match_copy = ext_nf.copy()
                match_copy["_match_score"] = score
                matches.append(match_copy)

        return matches

    def _handle_single_match(
        self,
        expected_nf: dict,
        extracted: dict,
        cnpj_start_dates: dict,
        page_categories: list[str],
    ) -> dict:
        """
        Handle case with single document match.

        :param expected_nf: Expected declaration dict.
        :param extracted: Single matched document.
        :param cnpj_start_dates: CNPJ → start_date cache.
        :param page_categories: Page categories list.
        :returns: Result dict with extracted document and classification.
        """
        ext_cnpj_norm = normalize_cnpj(extracted.get("cnpj_emitente", ""))
        ext_numero_norm = normalize_number(extracted.get("numero_nf", ""))
        ext_valor_norm = normalize_value(extracted.get("valor_total", 0.0))

        # Check if valor matches
        valor_match = abs(expected_nf["valor_norm"] - ext_valor_norm) < 0.01

        # Determine match type
        numero_match_type = "exact" if expected_nf["numero_norm"] == ext_numero_norm else "fuzzy"

        # Validate date against company start date
        data_envio = expected_nf["original"].get("data_envio")
        inicio_atividade = expected_nf["original"].get("cnpj_data_abertura") or cnpj_start_dates.get(ext_cnpj_norm)
        date_valid = check_date_against_company_start(data_envio, inicio_atividade)

        # Check for deduplication
        is_duplicate = self._is_duplicate_nf(
            ext_cnpj_norm,
            ext_numero_norm,
            expected_nf.get("cod_organizacao", ""),
            expected_nf.get("cod_unidade", ""),
            expected_nf.get("pdf_name", ""),
            expected_nf.get("id_documento"),
            expected_nf["original"].get("data_envio"),
        )

        # Compute classification
        rule_result = self._classify(
            nf_found=True,
            valor_pago=expected_nf["original"].get("valor_pago"),
            valor_documento=expected_nf["valor_norm"],
            valor_extracted=ext_valor_norm,
            tipo_documento=extracted.get("tipo_documento"),
            page_categories=page_categories,
            date_valid=date_valid,
            is_duplicate=is_duplicate,
            data_emissao_expected=expected_nf["original"].get("data_emissao"),
            data_emissao_extracted=extracted.get("data_emissao"),
            data_servico=extracted.get("data_servico"),
            cnpj_data_abertura=inicio_atividade,
        )

        return {
            "extracted": extracted,
            "expected": expected_nf["original"],
            "valor_match": valor_match,
            "match_quality": "PERFECT" if valor_match else "GOOD",
            "classification": rule_result.classification,
            "rule_name": rule_result.rule_name,
            "reason": rule_result.reason,
            "numero_match_type": numero_match_type,
        }

    def _handle_multiple_matches(
        self,
        expected_nf: dict,
        matches: list[dict],
        cnpj_start_dates: dict,
        page_categories: list[str],
    ) -> dict:
        """
        Handle case with multiple document matches (prioritization).

        :param expected_nf: Expected declaration dict.
        :param matches: List of matched documents.
        :param cnpj_start_dates: CNPJ → start_date cache.
        :param page_categories: Page categories list.
        :returns: Result dict with prioritized document and classification.
        """
        from .document_prioritizer import select_prioritized_document

        # Log warning about multiple matches
        types = [doc.get("tipo_documento") for doc in matches]
        logger.warning(
            f"Multiple documents match CNPJ {expected_nf['cnpj_norm']} + "
            f"Número {expected_nf['numero_norm']}: {types}. "
            f"Selecting by priority."
        )

        # Select prioritized document
        best_extracted = select_prioritized_document(matches)

        # Process as single match
        return self._handle_single_match(expected_nf, best_extracted, cnpj_start_dates, page_categories)

    def _handle_nf_ticket_merge(
        self,
        expected_nf: dict,
        nf: dict,
        ticket: dict,
        match_type: str,
        cnpj_start_dates: dict,
        page_categories: list[str],
    ) -> dict:
        """
        Handle NF + Ticket merge case.

        :param expected_nf: Expected declaration dict.
        :param nf: NF document.
        :param ticket: Ticket document.
        :param match_type: 'direct' or 'reverse'.
        :param cnpj_start_dates: CNPJ → start_date cache.
        :param page_categories: Page categories list.
        :returns: Result dict with merged document and classification.
        """
        from .document_merger import get_merge_justificativa, merge_nf_and_ticket
        from .rps_matcher import get_apontamento_leve_justification, should_apply_apontamento_leve

        # Merge documents
        merged = merge_nf_and_ticket(nf, ticket)

        # Generate justification
        justificativa = get_merge_justificativa(nf, ticket, merged)

        # Check if Apontamento Leve should be applied
        numero_declarado = expected_nf["original"].get("numero_nf", "")
        if should_apply_apontamento_leve(numero_declarado, nf, ticket, match_type):
            classificacao_especial = "Apontamento Leve"
            justificativa = get_apontamento_leve_justification(nf, ticket, numero_declarado)
        else:
            classificacao_especial = None

        # Process merged document as single match
        result = self._handle_single_match(expected_nf, merged, cnpj_start_dates, page_categories)

        # Override justification and classification if applicable
        result["merge_justificativa"] = justificativa
        if classificacao_especial:
            result["classification"] = classificacao_especial
            result["reason"] = justificativa

        return result

    def _handle_missing_nf(
        self,
        expected_nf: dict,
        page_categories: list[str],
    ) -> dict:
        """
        Handle case with no document match (missing NF).

        :param expected_nf: Expected declaration dict.
        :param page_categories: Page categories list.
        :returns: Result dict for missing NF.
        """
        # Check for deduplication
        is_duplicate = self._is_duplicate_nf(
            expected_nf["cnpj_norm"],
            expected_nf["numero_norm"],
            expected_nf.get("cod_organizacao", ""),
            expected_nf.get("cod_unidade", ""),
            expected_nf.get("pdf_name", ""),
            expected_nf.get("id_documento"),
            expected_nf["original"].get("data_envio"),
        )

        # Compute classification for missing NF
        rule_result = self._classify(
            nf_found=False,
            valor_pago=expected_nf["original"].get("valor_pago"),
            valor_documento=expected_nf["valor_norm"],
            valor_extracted=None,
            tipo_documento=None,
            page_categories=page_categories,
            is_duplicate=is_duplicate,
            data_emissao_expected=expected_nf["original"].get("data_emissao"),
            data_emissao_extracted=None,
        )

        missing_nf_data = expected_nf["original"].copy()
        missing_nf_data["classification"] = rule_result.classification
        missing_nf_data["rule_name"] = rule_result.rule_name
        missing_nf_data["reason"] = rule_result.reason

        return {
            "expected": missing_nf_data,
            "extracted": None,
            "classification": rule_result.classification,
            "rule_name": rule_result.rule_name,
            "reason": rule_result.reason,
        }

    def _process_single_declaration(
        self,
        expected_nf: dict,
        extracted_nfs: list[dict],
        cnpj_start_dates: dict,
        page_categories: list[str],
    ) -> dict:
        """
        Process a SINGLE declaration independently (declaração-centric).

        This is the core of the new strategy:
        1. Try RPS match first (NF + Ticket)
        2. If no RPS match, try standard match (CNPJ + número)
        3. Handle based on number of matches

        :param expected_nf: Expected declaration dict (already normalized).
        :param extracted_nfs: List of ALL extracted documents from PDF.
        :param cnpj_start_dates: CNPJ → start_date cache.
        :param page_categories: Page categories list.
        :returns: Result dict for this declaration.
        """
        from .rps_matcher import find_nf_ticket_by_rps

        # STEP 1: Try RPS match (NF + Ticket)
        nf, ticket, match_type = find_nf_ticket_by_rps(expected_nf["original"], extracted_nfs)

        if nf and ticket:
            # Case C: NF + Ticket merge
            logger.info(
                f"NF+Ticket merge detected for {expected_nf['cnpj_norm']} / "
                f"{expected_nf['numero_norm']} (match_type: {match_type})"
            )
            return self._handle_nf_ticket_merge(expected_nf, nf, ticket, match_type, cnpj_start_dates, page_categories)

        # STEP 2: Standard match (CNPJ + número + data)
        # Strategy: always try perfect matches (3/3) first.
        # If min_match_score < 3, fall back to partial matches when no perfect match found.
        # When min_match_score == 3, the fallback is skipped entirely.

        # First, try to find perfect matches (score = 3)
        perfect_matches = self._find_standard_matches(
            expected_nf["cnpj_norm"],
            expected_nf["numero_norm"],
            expected_nf["original"].get("data_emissao"),
            extracted_nfs,
            min_match_score=3,  # Require all 3 fields to match
        )

        # If no perfect match found, fall back to partial matches only when configured to do so
        if len(perfect_matches) == 0 and self.min_match_score < 3:
            standard_matches = self._find_standard_matches(
                expected_nf["cnpj_norm"],
                expected_nf["numero_norm"],
                expected_nf["original"].get("data_emissao"),
                extracted_nfs,
                min_match_score=self.min_match_score,  # Allow partial match per config
            )
        else:
            # Use perfect matches (or empty list when min_match_score == 3 and no 3/3 found)
            standard_matches = perfect_matches

        # STEP 3: Handle based on number of matches
        if len(standard_matches) == 0:
            # No match
            return self._handle_missing_nf(expected_nf, page_categories)

        elif len(standard_matches) == 1:
            # Case A: Single match
            return self._handle_single_match(expected_nf, standard_matches[0], cnpj_start_dates, page_categories)

        else:
            # Case B: Multiple matches (prioritization)
            return self._handle_multiple_matches(expected_nf, standard_matches, cnpj_start_dates, page_categories)
