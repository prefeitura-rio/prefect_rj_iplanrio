"""
Módulo de carregamento e caching dos artefatos estáticos de calibração.
Garante o uso da baseline congelada (LightGBM, Conformal CCCP e Centróides do Goldenset).
"""

from dataclasses import dataclass
from pathlib import Path
import sys
import types
from typing import Optional
import joblib

from ..constants import LOCAL_ARTIFACTS_DIR, PROMPTS_DIR, VERSAO_PROMPT_PADRAO
from .conformal import ArtefatosConforme
from .meta_calibrator import ModeloCalibrador


def _registrar_aliases_legados() -> None:
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


_BASELINE_CACHE: Optional[BaselineReferencia] = None


def carregar_prompt_sistema(
    caminho_prompt: Optional[Path] = None,
    gcs_uri: Optional[str] = None
) -> str:
    """Carrega o texto das instruções operacionais do GCS ou do disco local."""
    from ..constants import GCS_PROMPT_URI
    uri_alvo = gcs_uri or GCS_PROMPT_URI
    if uri_alvo and str(uri_alvo).startswith("gs://"):
        try:
            import shutil, subprocess
            cmd = shutil.which("gcloud")
            if cmd:
                res = subprocess.run(
                    [cmd, "storage", "cat", str(uri_alvo)],
                    capture_output=True,
                    encoding="utf-8",
                    check=True
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
    diretorio_artefatos: Optional[Path] = None,
) -> BaselineReferencia:
    """Carrega o Meta-Calibrador e os quantis conformais em memória uma única vez."""
    global _BASELINE_CACHE
    if _BASELINE_CACHE is not None and not forcar_recarga:
        return _BASELINE_CACHE

    _registrar_aliases_legados()
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

    if not arquivos_calibrador or not arquivos_conformal:
        raise FileNotFoundError(
            f"Artefatos .joblib de calibração não localizados em {pasta}."
        )

    calibrador = joblib.load(arquivos_calibrador[0])
    conformal = joblib.load(arquivos_conformal[0])
    prompt_texto = carregar_prompt_sistema()

    _BASELINE_CACHE = BaselineReferencia(
        calibrador=calibrador,
        conformal=conformal,
        prompt_sistema=prompt_texto,
    )
    return _BASELINE_CACHE
