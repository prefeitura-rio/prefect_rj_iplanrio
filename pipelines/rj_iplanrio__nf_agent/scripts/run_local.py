"""Processa PDFs locais com chamadas diretas ao Bifrost e grava o NDJSON no formato extracao_pagina.

Uso::

    uv run --package rj_iplanrio__nf_agent python pipelines/rj_iplanrio__nf_agent/scripts/run_local.py \\
        --pdf caminho/doc.pdf --output saida.ndjson

Requer no ``.env``: ``BIFROST_API_KEY``, ``BIFROST_BASE_URL`` e ao menos um
``PROMPT_CLASSIFICATION_V*`` e um ``PROMPT_EXTRACTION_V*``. Não usa GCS nem BigQuery.
"""

import argparse
import json
import logging
import sys
import uuid
from pathlib import Path

from dotenv import load_dotenv
from openai import OpenAI

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from pipelines.rj_iplanrio__nf_agent.utils.bifrost import build_client  # noqa: E402
from pipelines.rj_iplanrio__nf_agent.utils.direct import process_pdf_direct  # noqa: E402
from pipelines.rj_iplanrio__nf_agent.utils.output import (  # noqa: E402
    RunMetadata,
    build_extracao_pagina_rows,
    build_versao_pipeline,
    utc_now_naive,
)
from pipelines.rj_iplanrio__nf_agent.utils.pdf import PdfPages  # noqa: E402
from pipelines.rj_iplanrio__nf_agent.utils.prompts import load_prompts  # noqa: E402
from pipelines.rj_iplanrio__nf_agent.utils.results import build_pdf_results  # noqa: E402
from pipelines.rj_iplanrio__nf_agent.utils.versioning import compute_processing_version  # noqa: E402
from prefect_rj_iplanrio.logging import get_logger  # noqa: E402

logger = get_logger(__name__)


def parse_args() -> argparse.Namespace:
    """Lê os argumentos de linha de comando.

    :returns: Argumentos.
    """
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--pdf", type=Path, help="Um PDF local.")
    source.add_argument("--pdfs-dir", type=Path, help="Pasta com PDFs locais.")
    parser.add_argument("--output", type=Path, required=True, help="Arquivo NDJSON de saída.")
    parser.add_argument(
        "--env-file", type=Path, default=Path(__file__).resolve().parents[1] / ".env", help="Arquivo .env."
    )
    return parser.parse_args()


def run(pdf_paths: list[Path], output_path: Path, client: OpenAI) -> int:
    """Processa os PDFs e grava as linhas de saída.

    :param pdf_paths: PDFs locais.
    :param output_path: Arquivo NDJSON a gravar.
    :param client: Cliente do Bifrost.
    :returns: Número de linhas gravadas.
    """
    prompts = load_prompts()
    pdfs, classifications, extractions = [], [], []
    for path in pdf_paths:
        logger.info("Processando %s", path.name)
        result = process_pdf_direct(client, path.stem, path.read_bytes(), prompts)
        pdfs.append(PdfPages(path.stem, result.total_pages))
        classifications.extend(result.classifications)
        extractions.extend(result.extractions)

    metadata = RunMetadata(
        versao_pipeline=build_versao_pipeline(
            compute_processing_version(prompts),
            prompts.classification_version,
            prompts.extraction_version,
            f"local-{uuid.uuid4()}",
            None,
        ),
        generated_at=utc_now_naive(),
    )
    rows = build_extracao_pagina_rows(build_pdf_results(pdfs, classifications, extractions), metadata)
    output_path.write_text(
        "".join(json.dumps(row, ensure_ascii=False, default=str) + "\n" for row in rows), encoding="utf-8"
    )
    return len(rows)


def main() -> None:
    """Ponto de entrada da CLI."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    args = parse_args()
    if not args.env_file.exists():
        raise SystemExit(f".env não encontrado em {args.env_file}; use --env-file.")
    load_dotenv(args.env_file)
    pdf_paths = [args.pdf] if args.pdf else sorted(args.pdfs_dir.glob("*.pdf"))
    if not pdf_paths:
        raise SystemExit("Nenhum PDF encontrado.")
    count = run(pdf_paths, args.output, build_client())
    logger.info("%d linhas gravadas em %s", count, args.output)


if __name__ == "__main__":
    main()
