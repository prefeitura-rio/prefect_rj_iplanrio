"""Batch Prediction path for the NF Agent pipeline, routed through Bifrost.

This package is an *alternative* execution mode of the single
``rj_iplanrio__nf_agent`` flow (``execution_mode="batch"``, the default) —
it does not replace the synchronous mode (``execution_mode="sync"``,
per-request, also via Bifrost), which stays available for small/fast or
on-demand runs. It exists to process large backlogs at a lower cost than
per-request inference by submitting Bifrost Batch API jobs (which mirror
OpenAI's own Batch API shape) instead of making one Gemini call per page.

Both modes live in one pipeline directory/one ``@flow`` (see ``flow.py``'s
module docstring for why — briefly, one flow per directory per
``STYLEGUIDE.md`` §4.1, and this pipeline deliberately keeps both execution
paths rather than running two pipelines). Modules here freely import from
``..`` (GCS downloader, NF merge/coalesce, ``extracao_pagina`` row building,
prompts, page rendering) since it's genuinely shared with the synchronous
mode, not batch-specific, and a copy would drift.

Both modes now go through Bifrost with the *same* ``BIFROST_API_KEY``/
``BIFROST_BASE_URL`` (see ``utils/llm.py``) — this replaced an earlier
version of this package that talked to Vertex AI directly via the
``google-genai`` SDK. That direct-Vertex version was abandoned specifically
to keep all LLM traffic observable/governed through the company's Bifrost
gateway, even though it meant giving up Vertex's native BigQuery-sourced
batch I/O (passthrough ``pdf_name``/``page_number``/``session_id`` columns)
for Bifrost's JSONL-file-plus-``custom_id`` shape instead — see
``custom_id.py`` for how per-page identity is now carried.

Key architectural differences from the synchronous mode (see each module's
docstring for details):

- Classification and extraction are two separate, sequential batch jobs (the
  set of pages to extract is only known after classification results are
  in) — see ``poll.py`` for the state machine that walks a session through
  both phases, plus the next session's submit, all within one flow run
  without any blocking/sleeping while waiting on Bifrost.
- Batch input/output is JSONL (one line per page, ``custom_id`` +
  request/response body) uploaded and retrieved as Bifrost *files* — not
  BigQuery tables. Each page's PDF bytes are inlined as base64 directly in
  its JSONL row's request body (same ``file_data: data:application/pdf;base64,...``
  shape the synchronous path already sends live — see
  ``utils/extraction/api.py``), so there is no GCS scratch upload step
  (unlike the old direct-Vertex version, which needed one because Vertex's
  BigQuery-sourced batch requires a ``fileData.fileUri`` pointing at Cloud
  Storage).
- The per-run SQLite cache (``utils/cache.py::DatabaseManager``, used by
  sync mode) is not used on this path — dedup relies solely on
  ``utils.bigquery.PageStatusReader`` (already-done pages in
  ``extracao_pagina`` at the current pipeline version).
- Session/job tracking (``job_tracking.py``) still uses BigQuery
  (``nf_batch_jobs``, append-only event log) — this part of the
  architecture is unaffected by the Vertex-direct -> Bifrost move; only
  what gets tracked per event changed (``bifrost_batch_id``/
  ``input_file_id``/``output_file_id`` instead of ``vertex_job_name``/
  ``input_table``/``output_table``).
- Row-count budgeting: unlike Vertex AI's at-least-partially-documented
  200,000-request cap, Bifrost's own docs state no row or file-size limit
  for a batch job — see ``row_counting.py`` for why its default budget is
  now a conservative, unvalidated guess rather than a documented number.
"""
