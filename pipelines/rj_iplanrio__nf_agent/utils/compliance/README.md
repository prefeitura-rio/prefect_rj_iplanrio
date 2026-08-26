# `utils/compliance/`

Small package: normalization/matching helpers and the NFST↔Fatura cross-page
merger. Used by `utils/pipeline/metadata.py`'s `build_json_output` (the
pipeline's official per-page output, written to
`extracao_pagina` in BigQuery) and by `process_pdf`.

## What's here and why

| File | Used by | Purpose |
|---|---|---|
| `utils.py` | `metadata.py`, `nfst_fatura_cross_page_merger.py` | `normalize_cnpj`, `normalize_number`, `normalize_value`, `extract_core_numero`, `fuzzy_match_numero`, `parse_date_flexible`, `DocumentFields`, `match_score_3_fields` — the exact matching logic `build_json_output` uses to compute `match_id_documento` per page. |
| `nfst_fatura_cross_page_merger.py` | `process.py` (unconditionally, every PDF) | Links NFST pages to their Fatura page within the same telecom billing cycle, filling in `valor_total` on the NFST side. |

## What used to be here and isn't anymore

This package used to hold a full `ComplianceValidator` rule engine: a
BigQuery-backed deduplication lookup, RPS-based NF+Ticket merging, document
prioritization for multi-match cases, an 11-rule `RuleEngine`
(`OK`/`Suspect`/`Not Analyzable`/`Apontamento Leve` classification), and a
human-readable report builder — `core.py`, `matching.py`, `validate.py`,
`report.py`, `validator.py`, `document_merger.py`, `rps_matcher.py`,
`validation_context.py`, `rules/` (all deleted together).

**It was dead weight.** `process_pdf` called
`ComplianceValidator.validate_extraction()` on every single PDF — including a
real BigQuery query (`get_company_start_date`) per unique extracted CNPJ — but
`build_json_output` (the pipeline's only supported output format — JSON is
now hardcoded, the old `output_mode="excel"` CSV path and everything that only
existed to feed it, including `document_prioritizer.py`, were removed) never
read the result. It computed `match_id_documento` itself, independently, via
`match_score_3_fields` directly against the input declarations — the entire
rule-engine's output was thrown away, unread, after paying for it in
BigQuery calls and CPU on every run.

If a rule-engine-style classification is needed again in the future, it
belongs downstream in BigQuery (the `despesa_classificada` view already
computes 7 boolean indicators in SQL from `extracao_pagina` + external
sources — see `../../DESPESA_CLASSIFICADA.md`), not recomputed per-PDF in
Python for a result nothing reads.
