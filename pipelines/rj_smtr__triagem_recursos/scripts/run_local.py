"""Local execution entrypoint for rj_smtr__triagem_recursos pipeline.

Runs the triage flow locally without triggering deployments.
Adheres to Styleguide Section 4.1 & 6.4 (no `if __name__ == '__main__':` in flow.py).
"""

import argparse
from pathlib import Path
import sys

try:
    from dotenv import find_dotenv, load_dotenv
    load_dotenv(find_dotenv(usecwd=True))
except ImportError:
    pass

SCRIPT_DIR = Path(__file__).resolve().parent
PIPELINE_DIR = SCRIPT_DIR.parent
REPO_ROOT = PIPELINE_DIR.parent.parent

if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from pipelines.rj_smtr__triagem_recursos.flow import rj_smtr__triagem_recursos


def parse_args() -> argparse.Namespace:
    """Parse command line arguments for local execution."""
    parser = argparse.ArgumentParser(
        description="Executa o flow de triagem SMTR localmente ou integrado ao SEI."
    )
    parser.add_argument(
        "--local",
        action="store_true",
        default=True,
        help="Executa em modo local com SQLite/Postgres local.",
    )
    parser.add_argument(
        "--cloud",
        action="store_false",
        dest="local",
        help="Executa em modo nuvem (Cloud SQL).",
    )
    parser.add_argument(
        "--fonte-pdf",
        default="local",
        choices=["local", "sei"],
        help="Origem dos PDFs: 'local' (pasta pdfs/) ou 'sei' (download em memória).",
    )
    parser.add_argument(
        "--modo",
        default="INCREMENTAL",
        choices=["INCREMENTAL", "PASSIVO"],
        help="Modo de carga.",
    )
    parser.add_argument(
        "--limite",
        type=int,
        default=5,
        help="Quantidade máxima de processos no lote de teste.",
    )
    parser.add_argument(
        "--reasoning",
        default="medium",
        choices=["high", "medium", "low", "off"],
        help="Nível de raciocínio da LLM (Gemini 2.5/Flash).",
    )
    parser.add_argument(
        "--executar-n2",
        action="store_true",
        default=False,
        help="Habilita execução de N=2 na faixa de incerteza moderada.",
    )
    return parser.parse_args()


def main() -> None:
    """Execute flow with parsed arguments."""
    args = parse_args()
    rj_smtr__triagem_recursos(
        is_local=args.local,
        fonte_pdf=args.fonte_pdf,
        modo_execucao=args.modo,
        limite_lote=args.limite,
        reasoning_effort=args.reasoning,
        executar_n2=args.executar_n2,
    )


if __name__ == "__main__":
    main()
