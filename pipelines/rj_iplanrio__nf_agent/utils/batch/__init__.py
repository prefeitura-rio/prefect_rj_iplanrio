"""Vertex AI Batch Prediction path for the NF Agent pipeline.

This package is an *alternative* execution mode of the single
``rj_iplanrio__nf_agent`` flow (``execution_mode="batch"``, the default) —
it does not replace the synchronous mode (``execution_mode="sync"``,
Bifrost/OpenAI-protocol, per-request), which stays available for small/fast
or on-demand runs. It exists to process large backlogs at 50% of the
online-inference cost by submitting Vertex AI Batch Prediction jobs instead
of making one Gemini call per page.

Both modes live in one pipeline directory/one ``@flow`` (see ``flow.py``'s
module docstring for why — briefly, one flow per directory per
``STYLEGUIDE.md`` §4.1, and this pipeline deliberately keeps both execution
paths rather than running two pipelines). Modules here freely import from
``..`` (GCS downloader, NF merge/coalesce, ``extracao_pagina`` row building,
prompts, page rendering) since it's genuinely shared with the synchronous
mode, not batch-specific, and a copy would drift.

Key architectural differences from the synchronous mode (see each module's
docstring for details):

- LLM calls go straight to Vertex AI (``google-genai`` SDK, ``enterprise=True``)
  instead of through the Bifrost gateway — there is no batch route through
  Bifrost. This reintroduces a direct Vertex AI dependency that the
  synchronous mode deliberately avoids (see ``utils/llm.py``'s docstring for
  why); batch jobs may hit the same ``constraints/vertexai.allowedModels``
  Org Policy block that motivated that avoidance — this is a known, accepted
  risk, not yet validated against the real GCP project.
- Classification and extraction are two separate, sequential batch jobs (the
  set of pages to extract is only known after classification results are
  in) — see ``poll.py`` for the state machine that walks a session through
  both phases, plus the next session's submit, all within one flow run
  without any blocking/sleeping while waiting on Vertex AI.
- BigQuery (not Cloud Storage/JSONL) is used as the batch job's input/output
  — this lets extra tracking columns (``pdf_name``, ``page_number``,
  ``session_id``) pass through untouched into the output table, which is how
  each output row is matched back to the page that produced it (the
  Cloud-Storage-source path has no documented per-row identifier).
- The per-run SQLite cache (``utils/cache.py::DatabaseManager``, used by
  sync mode) is not used on this path — dedup relies solely on
  ``utils.bigquery.PageStatusReader`` (already-done pages in
  ``extracao_pagina`` at the current pipeline version).
- Row-count budgeting: Vertex AI Batch Prediction caps a single job at
  200,000 requests (undocumented whether this cap is identical for the
  BigQuery-sourced path — treated as the working assumption, with margin).
  Since a PDF's page count is unknown until it is opened, session sizing is
  done by accumulating actual page counts across candidate PDFs (see
  ``row_counting.py``), not by capping the number of PDFs.
"""
