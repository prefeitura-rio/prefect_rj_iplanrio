"""Baixe e normalize artefatos para inspeção local, sem escrever no BigQuery."""

from __future__ import annotations

import argparse
import csv
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pipelines.rj_crm__agent_quality_registry.utils import parser
from pipelines.rj_crm__agent_quality_registry.utils.gitlab import discover_artifacts
from prefect_rj_iplanrio.logging import get_logger

logger = get_logger(__name__)


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    """Escreva linhas normalizadas em um arquivo CSV local.

    :param path: Caminho do arquivo CSV a criar.
    :param rows: Linhas normalizadas a persistir.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fieldnames = list(dict.fromkeys(key for row in rows for key in row))
    with path.open("w", newline="", encoding="utf-8") as output:
        writer = csv.DictWriter(output, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    """Exporte artefatos do Registry para inspeção local.

    :raises requests.HTTPError: Se a API GitLab responder com erro HTTP.
    :raises ValueError: Se algum artefato tiver schema não suportado.
    """
    cli = argparse.ArgumentParser(description=__doc__)
    cli.add_argument("--output-dir", default="tmp/agent-quality-export")
    args = cli.parse_args()
    output_dir = Path(args.output_dir)
    aggregates: list[dict[str, Any]] = []
    details: list[dict[str, Any]] = []
    now = datetime.now(timezone.utc)

    artifacts = discover_artifacts()
    for registry_file, artifact in artifacts:
        aggregate, artifact_details = parser.parse_artifact(registry_file, artifact, now)
        aggregates.append(aggregate)
        details.extend(artifact_details)

    write_csv(output_dir / "agent_quality_versions.csv", aggregates)
    write_csv(output_dir / "agent_quality_test_result_details.csv", details)

    qa = [row for row in aggregates if row.get("environment_scope") == "qa"]
    prod = [row for row in aggregates if row.get("environment_scope") == "prod_baseline"]
    write_csv(output_dir / "agent_quality_qa_versions.csv", qa)
    write_csv(output_dir / "agent_quality_prod_baselines.csv", prod)
    logger.info("Artefatos exportados: %d", len(artifacts))
    logger.info("QA: %d | PROD: %d | detalhes: %d", len(qa), len(prod), len(details))
    logger.info("Saída: %s", output_dir.resolve())


if __name__ == "__main__":
    main()
