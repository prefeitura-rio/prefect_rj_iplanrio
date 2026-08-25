"""
OCR-based NF Classifier - Uses keyword/regex matching on OCR text.
Requires OCR preprocessing before classification.
"""

import re

from ..base import BaseClassifier
from ..config import BEST_PARAMS, load_categories

# Pattern definitions for regex-based matching
PATTERNS = {
    "PATTERN:CNPJ": r"\b\d{2}\.\d{3}\.\d{3}/\d{4}-\d{2}\b",
    "PATTERN:CPF": r"\b\d{3}\.\d{3}\.\d{3}-\d{2}\b",
    "PATTERN:DATE": r"\b\d{2}/\d{2}/\d{4}\b",
    "PATTERN:MONEY": r"R\$\s*[\d.,]+(?:\s*\d+)?",
    "PATTERN:CEP": r"\b\d{5}-?\d{3}\b",
    "PATTERN:CFOP": r"\b\d{4}\b(?=.*(?:cfop|natureza|operação))",
    "PATTERN:INSCRICAO_ESTADUAL": r"(?i)inscri[çc][aã]o\s*estadual[:\s]*[\d./-]+",
    "PATTERN:INSCRICAO_MUNICIPAL": r"(?i)inscri[çc][aã]o\s*municipal[:\s]*[\d./-]+",
    "PATTERN:NFE_ACCESS_KEY": r"\b\d{44}\b",
    "PATTERN:NF_NUMBER": r"(?i)(?:n[úu]mero|n[°º])\s*(?:da\s*)?(?:nota|nf)[:\s]*\d+",
    "PATTERN:RPS_NUMBER": r"(?i)rps[:\s]*\d+",
    "PATTERN:TAX_CODE": r"(?i)c[óo]digo\s*(?:de\s*)?verifica[çc][aã]o[:\s]*[\w-]+",
    "PATTERN:VERIFICATION_CODE": r"(?i)c[óo]digo[:\s]*[\w]{8,}",
}


def count_pattern_matches(text: str, pattern_name: str) -> int:
    """Count how many times a regex pattern matches in the text."""
    if pattern_name not in PATTERNS:
        return 0
    pattern = PATTERNS[pattern_name]
    matches = re.findall(pattern, text, re.IGNORECASE)
    return len(matches)


def count_sequence_matches(text: str, sequence: str) -> int:
    """Count occurrences of a sequence (literal or pattern) in text."""
    if sequence.startswith("PATTERN:"):
        return count_pattern_matches(text, sequence)
    else:
        # Literal text match (case insensitive)
        return text.lower().count(sequence.lower())


def calculate_score(text: str, categories: dict, params: dict | None = None) -> float:
    """
    Calculate weighted score for a page based on sequence matches.

    :param text: OCR text from the page.
    :param categories: Dictionary with category sequences.
    :param params: Classifier parameters (weights).
    :returns: Weighted score (positive = likely NF, negative = likely non-NF).
    """
    if params is None:
        params = BEST_PARAMS

    score = 0.0
    text_lower = text.lower()

    # NF-specific categories
    nf_specific = categories.get("NF-specific", {})

    # High confidence NF indicators
    for seq in nf_specific.get("high_confidence", []):
        count = count_sequence_matches(text_lower, seq)
        if count > 0:
            score += count * params.get("weight_NF-specific_high_confidence", 23)

    # Medium confidence NF indicators
    for seq in nf_specific.get("medium_confidence", []):
        count = count_sequence_matches(text_lower, seq)
        if count > 0:
            score += count * params.get("weight_NF-specific_medium_confidence", 5)

    # Low confidence NF indicators
    for seq in nf_specific.get("low_confidence", []):
        count = count_sequence_matches(text_lower, seq)
        if count > 0:
            score += count * params.get("weight_NF-specific_low_confidence", 1)

    # Common sequences (neutral weight)
    for seq in categories.get("Common", []):
        count = count_sequence_matches(text_lower, seq)
        if count > 0:
            score += count * params.get("weight_Common", 0)

    # Non-NF indicators (negative weight)
    for seq in categories.get("Non-NF", []):
        count = count_sequence_matches(text_lower, seq)
        if count > 0:
            score += count * params.get("weight_Non-NF", -13)

    return score


def classify_page(text: str, categories: dict, params: dict | None = None) -> tuple[str, float]:
    """
    Classify a single page as NF or Non-NF.

    :param text: OCR text from the page.
    :param categories: Dictionary with category sequences.
    :param params: Classifier parameters.
    :returns: Tuple of (classification, score).
    """
    if params is None:
        params = BEST_PARAMS

    score = calculate_score(text, categories, params)

    threshold_nf = params.get("threshold_NF", 2.5)
    threshold_non_nf = params.get("threshold_NonNF", 0)

    if score >= threshold_nf:
        return "NF", score
    elif score <= threshold_non_nf:
        return "Non-NF", score
    else:
        return "Uncertain", score


class NFClassifier(BaseClassifier):
    """
    Weighted sequence classifier for Nota Fiscal pages.
    OCR-based: requires text input from OCR preprocessing.
    """

    def __init__(self, params: dict | None = None, categories: dict | None = None):
        """
        Initialize classifier with parameters and categories.

        :param params: Classification parameters (weights and thresholds).
        :param categories: Sequence categories dictionary.
        """
        self.params = params or BEST_PARAMS
        self.categories = categories

        if self.categories is None:
            try:
                self.categories = load_categories()
            except FileNotFoundError:
                raise ValueError("Categories not provided and categories file not found")

    @property
    def requires_ocr(self) -> bool:
        """This classifier requires OCR text input."""
        return True

    def classify(self, text: str) -> tuple[str, float]:
        """
        Classify a page text as NF or Non-NF.

        :param text: OCR text from the page.
        :returns: Tuple of (classification, score).
        """
        return classify_page(text, self.categories, self.params)

    def classify_pages(self, page_texts: list[str]) -> list[dict]:
        """
        Classify multiple pages.

        :param page_texts: List of OCR texts, one per page.
        :returns: List of classification results with page info.
        """
        results = []

        for idx, text in enumerate(page_texts):
            classification, score = self.classify(text)
            results.append(
                {"page": idx + 1, "classification": classification, "score": score, "is_nf": classification == "NF"}
            )

        return results

    def get_nf_pages(self, page_texts: list[str]) -> list[int]:
        """
        Get list of page numbers classified as NF.

        :param page_texts: List of OCR texts.
        :returns: List of 1-indexed page numbers that are NFs.
        """
        results = self.classify_pages(page_texts)
        return [r["page"] for r in results if r["is_nf"]]

    def calculate_score(self, text: str) -> float:
        """
        Calculate the weighted score for a page.

        :param text: OCR text.
        :returns: Weighted score.
        """
        return calculate_score(text, self.categories, self.params)

    def get_sequence_breakdown(self, text: str) -> dict:
        """
        Get detailed breakdown of sequence matches and their contributions.

        :param text: OCR text.
        :returns: Dictionary with match details per category.
        """
        text_lower = text.lower()
        breakdown = {
            "NF-specific_high_confidence": [],
            "NF-specific_medium_confidence": [],
            "NF-specific_low_confidence": [],
            "Common": [],
            "Non-NF": [],
        }

        nf_specific = self.categories.get("NF-specific", {})

        # High confidence
        for seq in nf_specific.get("high_confidence", []):
            count = count_sequence_matches(text_lower, seq)
            if count > 0:
                weight = self.params.get("weight_NF-specific_high_confidence", 23)
                breakdown["NF-specific_high_confidence"].append(
                    {"sequence": seq, "count": count, "weight": weight, "contribution": count * weight}
                )

        # Medium confidence
        for seq in nf_specific.get("medium_confidence", []):
            count = count_sequence_matches(text_lower, seq)
            if count > 0:
                weight = self.params.get("weight_NF-specific_medium_confidence", 5)
                breakdown["NF-specific_medium_confidence"].append(
                    {"sequence": seq, "count": count, "weight": weight, "contribution": count * weight}
                )

        # Low confidence
        for seq in nf_specific.get("low_confidence", []):
            count = count_sequence_matches(text_lower, seq)
            if count > 0:
                weight = self.params.get("weight_NF-specific_low_confidence", 1)
                breakdown["NF-specific_low_confidence"].append(
                    {"sequence": seq, "count": count, "weight": weight, "contribution": count * weight}
                )

        # Common
        for seq in self.categories.get("Common", []):
            count = count_sequence_matches(text_lower, seq)
            if count > 0:
                weight = self.params.get("weight_Common", 0)
                breakdown["Common"].append(
                    {"sequence": seq, "count": count, "weight": weight, "contribution": count * weight}
                )

        # Non-NF
        for seq in self.categories.get("Non-NF", []):
            count = count_sequence_matches(text_lower, seq)
            if count > 0:
                weight = self.params.get("weight_Non-NF", -13)
                breakdown["Non-NF"].append(
                    {"sequence": seq, "count": count, "weight": weight, "contribution": count * weight}
                )

        return breakdown
