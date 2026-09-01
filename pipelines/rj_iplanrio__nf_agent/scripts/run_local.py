"""Local test entrypoint — runs classification/extraction on PDFs already
downloaded to disk, calling only the LLM (Gemini via Bifrost). No GCS or
BigQuery client is ever instantiated: ``POCProcessor.process_pdf`` only
touches ``GCSDownloader`` when ``pdf_path`` is ``None`` (see
``utils/processing/process.py``), and neither ``PageStatusReader`` nor
``BigQueryWriter`` are reachable from that call path at all — those only
run inside ``utils/pipeline.py::discover_pending_files`` and
``utils/orchestration.py::write_run_summary``, neither of which this
script calls.

Usage::

    uv run --package rj_iplanrio__nf_agent python scripts/run_local.py \\
        --pdfs-dir /caminho/para/pdfs/locais \\
        --output ./local_run_output.ndjson

A single file also works: ``--pdf /caminho/para/um.pdf``.

Requires (from ``--env-file``, default ``.env`` next to this script):
``BIFROST_API_KEY``, ``BIFROST_BASE_URL`` (pointed at Bifrost's OpenAI-compatible
endpoint, e.g. ``https://bifrost.iplan.dados.rio/openai/v1`` — see ``utils/llm.py``),
and at least one ``PROMPT_CLASSIFICATION_V*``/``PROMPT_EXTRACTION_V*`` pair.
``GCS_BUCKET`` and ``RJ_NF_AGENT_CREDENTIALS`` are NOT needed for this script.
The ``openai`` SDK is a normal dependency (``uv sync`` installs it) — no
isolated install needed.
"""

import argparse
import json
import os
import sys
import tempfile
from pathlib import Path
from unittest.mock import MagicMock

from dotenv import load_dotenv

SCRIPT_DIR = Path(__file__).resolve().parent
PACKAGE_DIR = SCRIPT_DIR.parent
REPO_ROOT = PACKAGE_DIR.parent.parent  # pipelines/rj_iplanrio__nf_agent/scripts -> repo root

# Everything here is imported as `pipelines.rj_iplanrio__nf_agent.*` (relative imports
# inside the package require it) — same layout tests/conftest.py relies on via
# pytest's `pythonpath = ["../.."]`. A plain script needs this on sys.path itself.
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

REQUIRED_STATIC_ENV_VARS = ("BIFROST_API_KEY", "BIFROST_BASE_URL")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    pdf_group = parser.add_mutually_exclusive_group(required=True)
    pdf_group.add_argument("--pdfs-dir", type=Path, help="Directory of local PDFs to process.")
    pdf_group.add_argument("--pdf", type=Path, help="A single local PDF to process.")
    parser.add_argument(
        "--output", type=Path, required=True, help="Where to write the extracao_pagina-shaped NDJSON output."
    )
    parser.add_argument(
        "--db-path",
        type=Path,
        default=Path("./local_run_cache.db"),
        help="SQLite cache path (default: ./local_run_cache.db). Reusing it across runs "
        "skips already-processed pages/PDFs.",
    )
    parser.add_argument(
        "--env-file",
        type=Path,
        default=SCRIPT_DIR / ".env" if (SCRIPT_DIR / ".env").exists() else PACKAGE_DIR / ".env",
        help="Path to the .env file to load before importing the pipeline (default: pipeline's own .env).",
    )
    return parser.parse_args()


def check_required_env_vars() -> None:
    """Fail fast with a clear message instead of the generic RuntimeError/
    FileNotFoundError that would otherwise come from deep inside llm.py/prompts.py.
    """
    # Imported here, after load_dotenv() has already run in main().
    from pipelines.rj_iplanrio__nf_agent.utils.prompts import (  # noqa: PLC0415
        list_available_versions,
        load_prompt_version,
    )

    missing = [var for var in REQUIRED_STATIC_ENV_VARS if not os.environ.get(var)]
    for prompt_type in ("classification", "extraction"):
        versions = list_available_versions(prompt_type)
        # `list_available_versions` only checks the env var *exists* — the .env
        # template ships with empty placeholders, so also check the latest
        # version actually has non-empty text (the real failure mode right now).
        latest_nonempty = versions and load_prompt_version(prompt_type, versions[-1])
        if not latest_nonempty:
            missing.append(f"PROMPT_{prompt_type.upper()}_V* (com texto preenchido, não vazio)")
    if missing:
        sys.exit(
            "Faltam variáveis de ambiente necessárias para rodar localmente: "
            f"{', '.join(missing)}. Preencha o .env (veja --env-file) antes de rodar."
        )


def main() -> None:
    args = parse_args()

    if not args.env_file.exists():
        sys.exit(f".env não encontrado em {args.env_file} — passe --env-file explicitamente.")
    load_dotenv(args.env_file)

    # Everything below must be imported only after load_dotenv() — gemini_classifier.py /
    # extraction/auth.py read PROMPT_* env vars at import time (see module docstring).
    check_required_env_vars()

    from iplanrio_agent_toolkit.rate_limiter import initialize_rate_limiter  # noqa: PLC0415

    from pipelines.rj_iplanrio__nf_agent.utils.cache import DatabaseManager  # noqa: PLC0415
    from pipelines.rj_iplanrio__nf_agent.utils.processing import metadata  # noqa: PLC0415
    from pipelines.rj_iplanrio__nf_agent.utils.processing.batch import _log_processing_summary  # noqa: PLC0415
    from pipelines.rj_iplanrio__nf_agent.utils.processing.processor import POCProcessor  # noqa: PLC0415

    pdf_paths = [args.pdf] if args.pdf else sorted(args.pdfs_dir.glob("*.pdf"))
    if not pdf_paths:
        sys.exit(f"Nenhum PDF encontrado em {args.pdfs_dir or args.pdf}.")

    # Conservative defaults for a local/manual test run — not the full production rate.
    max_concurrent = 5
    requests_per_minute = 60
    initialize_rate_limiter(max_concurrent=max_concurrent, requests_per_minute=requests_per_minute)

    processor = POCProcessor(
        db_manager=DatabaseManager(args.db_path),  # SQLite only — no GCP.
        gcs_downloader=MagicMock(),  # never invoked: every process_pdf call below passes pdf_path explicitly.
        temp_dir=Path(tempfile.mkdtemp(prefix="nf_agent_local_run_")),
        prompt_versions=None,  # resolves to the latest available version, same as real runs.
    )

    print(f"Processing {len(pdf_paths)} PDF(s) locally (no GCS/BQ) — cache: {args.db_path}")
    pdf_tasks = [{"pdf_name": p.stem} for p in pdf_paths]
    results: dict[str, dict] = {}
    for path in pdf_paths:
        print(f"  → {path.name}")
        results[path.stem] = processor.process_pdf(pdf_filename=path.stem, pdf_path=path)

    extracao_pagina_rows = metadata.build_extracao_pagina_rows(
        pdf_tasks=pdf_tasks,
        pdf_results=results,
        timestamp_geracao=metadata.utc_now_naive(),
        versao_pipeline=metadata.build_versao_pipeline(
            workers=1,
            requests_per_minute=requests_per_minute,
            max_concurrent=max_concurrent,
        ),
        versao_prompt=metadata.build_versao_prompt(processor),
    )

    with args.output.open("w", encoding="utf-8") as f:
        for row in extracao_pagina_rows:
            f.write(json.dumps(row, ensure_ascii=False, default=str) + "\n")

    _log_processing_summary(len(pdf_tasks), extracao_pagina_rows)
    print(f"\nOutput (formato extracao_pagina) escrito em: {args.output}")


if __name__ == "__main__":
    main()
