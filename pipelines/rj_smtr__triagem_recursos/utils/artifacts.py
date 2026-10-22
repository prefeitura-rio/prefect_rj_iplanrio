"""Módulo de carregamento e caching dos artefatos estáticos de calibração.

Garante o uso da baseline congelada (LightGBM, Conformal CCCP e Centróides do Goldenset).
"""

from dataclasses import dataclass
from pathlib import Path
import sys
import types
from typing import Any
import joblib
from prefect_rj_iplanrio.logging import get_logger

from ..constants import (
    LOCAL_ARTIFACTS_DIR,
    PROMPTS_DIR,
    VERSAO_PROMPT_PADRAO,
    resolver_banco_url,
)
from .conformal import ArtefatosConforme, criar_artefatos_conforme_do_banco
from .meta_calibrator import (
    ModeloCalibrador,
    criar_calibrador_dos_parametros_banco,
)

logger = get_logger(__name__)


def registrar_aliases_legados() -> None:
    """Registra aliases em sys.modules para desserialização transparente de joblib."""
    from . import conformal, feature_extractor, meta_calibrator

    if "src" not in sys.modules:
        sys.modules["src"] = types.ModuleType("src")
    if "src.core" not in sys.modules:
        sys.modules["src.core"] = types.ModuleType("src.core")

    sys.modules["src.core.meta_calibrator"] = meta_calibrator
    sys.modules["src.core.feature_extractor"] = feature_extractor
    sys.modules["src.core.conformal"] = conformal


@dataclass
class BaselineReferencia:
    """Container dos modelos e quantis estatísticos congelados da v3."""

    calibrador: ModeloCalibrador
    conformal: ArtefatosConforme
    prompt_sistema: str


BASELINE_CACHE: BaselineReferencia | None = None


def carregar_prompt_sistema(
    caminho_prompt: Path | None = None,
    gcs_uri: str | None = None,
) -> str:
    """Carrega o texto das instruções operacionais do GCS ou do disco local.

    :param caminho_prompt: Caminho local opcional do arquivo de prompt.
    :param gcs_uri: URI opcional gs:// do arquivo no Google Cloud Storage.
    :returns: Conteúdo do prompt como string.
    :raises FileNotFoundError: Se nenhum prompt for localizado.
    """
    from ..constants import GCS_PROMPT_URI

    uri_alvo = gcs_uri or GCS_PROMPT_URI
    if uri_alvo and str(uri_alvo).startswith("gs://"):
        try:
            import shutil
            import subprocess

            cmd = shutil.which("gcloud")
            if cmd:
                res = subprocess.run(
                    [cmd, "storage", "cat", str(uri_alvo)],
                    capture_output=True,
                    encoding="utf-8",
                    check=True,
                )
                if res.stdout and len(res.stdout.strip()) > 100:
                    return res.stdout
        except Exception:
            pass

    if caminho_prompt and caminho_prompt.exists():
        return caminho_prompt.read_text(encoding="utf-8")

    padrao = PROMPTS_DIR / f"{VERSAO_PROMPT_PADRAO}.txt"
    if padrao.exists():
        return padrao.read_text(encoding="utf-8")

    arquivos = sorted(PROMPTS_DIR.glob("*.txt"))
    if arquivos:
        return arquivos[0].read_text(encoding="utf-8")

    raise FileNotFoundError("Nenhum arquivo de prompt localizado em prompts/.")


def carregar_baseline_referencia(
    forcar_recarga: bool = False,
    diretorio_artefatos: Path | None = None,
) -> BaselineReferencia:
    """Carrega o Meta-Calibrador e os quantis conformais em memória uma única vez.

    :param forcar_recarga: Se True, ignora o cache em memória e recarrega os arquivos.
    :param diretorio_artefatos: Diretório customizado onde os arquivos .joblib residem.
    :returns: Instância de BaselineReferencia com modelos e quantis.
    :raises FileNotFoundError: Se os arquivos de modelo não forem encontrados.
    """
    global BASELINE_CACHE
    if BASELINE_CACHE is not None and not forcar_recarga:
        return BASELINE_CACHE

    registrar_aliases_legados()
    pasta = diretorio_artefatos or LOCAL_ARTIFACTS_DIR

    arquivos_calibrador = sorted(
        pasta.glob("meta_calibrator_*.joblib"),
        key=lambda x: x.stat().st_mtime,
        reverse=True,
    )
    arquivos_conformal = sorted(
        pasta.glob("conformal_artifacts_*.joblib"),
        key=lambda x: x.stat().st_mtime,
        reverse=True,
    )

    if not arquivos_calibrador:
        raise FileNotFoundError(
            f"Nenhum arquivo meta_calibrator_*.joblib encontrado em {pasta.resolve()}"
        )
    if not arquivos_conformal:
        raise FileNotFoundError(
            f"Nenhum arquivo conformal_artifacts_*.joblib encontrado em {pasta.resolve()}"
        )

    calibrador: ModeloCalibrador = joblib.load(arquivos_calibrador[0])
    conformal: ArtefatosConforme = joblib.load(arquivos_conformal[0])
    prompt_sistema = carregar_prompt_sistema()

    BASELINE_CACHE = BaselineReferencia(
        calibrador=calibrador,
        conformal=conformal,
        prompt_sistema=prompt_sistema,
    )
    return BASELINE_CACHE


def carregar_artefatos_pipeline(
    is_local: bool = True,
    arquivo_prompt: str = f"{VERSAO_PROMPT_PADRAO}.txt",
) -> dict[str, Any]:
    """Carrega os artefatos da pipeline via PostgreSQL ativo com fallback para arquivos locais.

    :param is_local: Flag de ambiente local versus nuvem.
    :param arquivo_prompt: Nome do arquivo de prompt a ser carregado.
    :returns: Dicionário contendo calibrador, conformal, prompt e metadados de versão.
    """
    from .db_repository import buscar_configuracao_calibracao_ativa

    db_url = resolver_banco_url(is_local)
    calibrador = None
    conformal = None
    calibracao_id = None
    versao_modelo = "baseline_congelada_v3"

    try:
        logger.info("Recuperando configuração de calibração ativa no PostgreSQL...")
        config_db = buscar_configuracao_calibracao_ativa(db_url)
        calibrador = criar_calibrador_dos_parametros_banco(config_db)
        conformal = criar_artefatos_conforme_do_banco(config_db)
        calibracao_id = config_db.get("calibracao_id")
        versao_modelo = config_db.get("versao_modelo", "db_ativa")
        logger.info(
            "Configuração ativa do banco carregada | Versão: %s | ID: %s",
            versao_modelo,
            calibracao_id,
        )
    except Exception as exc:
        logger.warning(
            "Configuração no banco indisponível (%s). Usando baseline congelada...",
            exc,
        )
        baseline = carregar_baseline_referencia()
        calibrador = baseline.calibrador
        conformal = baseline.conformal

    caminho_prompt = PROMPTS_DIR / arquivo_prompt
    if not caminho_prompt.exists():
        caminho_prompt = PROMPTS_DIR / f"{VERSAO_PROMPT_PADRAO}.txt"

    prompt_sistema = caminho_prompt.read_text(encoding="utf-8")
    versao_prompt = caminho_prompt.stem

    return {
        "calibrador": calibrador,
        "conformal": conformal,
        "prompt_sistema": prompt_sistema,
        "calibracao_id": calibracao_id,
        "versao_modelo": versao_modelo,
        "versao_prompt": versao_prompt,
    }
