"""
POC Pipeline - Production system for processing NF database with GCS integration and caching.

Migrated from agent-nf-validator (run_poc/) by mechanical move. The heavy
``google.generativeai``-dependent modules (processor, run_pipeline) are imported
lazily so this package stays importable without the optional ``gemini`` extra.
"""
