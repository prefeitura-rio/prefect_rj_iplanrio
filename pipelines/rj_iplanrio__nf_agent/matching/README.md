# `matching/`

Small package: the NFST↔Fatura cross-page merger and the value-normalization
helper it depends on. Used by `process_pdf` (`processing/process.py`),
unconditionally on every PDF.

## What's here and why

| File | Used by | Purpose |
|---|---|---|
| `scoring.py` | `nfst_fatura_merger.py` | `normalize_value` — normalizes the Fatura's `valor_total` before propagating it to the linked NFST page. |
| `nfst_fatura_merger.py` | `process.py` (unconditionally, every PDF) | Links NFST pages to their Fatura page within the same telecom billing cycle, filling in `valor_total` on the NFST side. |

## What used to be here and isn't anymore

**Declaration-vs-extracted matching (`match_id_documento`).** `scoring.py`
used to also hold `normalize_cnpj`, `normalize_number`, `extract_core_numero`,
`fuzzy_match_numero`, `parse_date_flexible`, `DocumentFields` and
`match_score_3_fields` — the logic `processing/metadata.py`'s
`build_json_output` used to compare each extracted document against BigQuery
declarations (CNPJ + número + data, 2-of-3 fuzzy match) and populate a
`match_id_documento` field per page. This is a fundamentally different kind
of match than the NFST↔Fatura merger above: it compares a declaration
against an extraction, not two pages of the same PDF. It was removed because
that matching now happens entirely as BigQuery post-processing, not in this
pipeline — `extracao_pagina` no longer carries a `match_id_documento` field.
Note `queries/despesa_classificada.sql` (the downstream view) still expects
that field and needs to be rewritten as part of the same migration.

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
