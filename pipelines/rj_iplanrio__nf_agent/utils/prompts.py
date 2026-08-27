"""
Prompts module - Contains all prompts used by the NF processing pipeline.

Prompt text is not committed to the repo (it's not public) — each version
is an env var, injected at runtime from Infisical the same way
``RJ_NF_AGENT_CREDENTIALS``/``BIFROST_API_KEY`` already are (k8s secret,
one Infisical environment per deployment — teste/prod).

Versioning:
    Env var naming convention: ``PROMPT_{TIPO}_{VERSAO}`` (uppercase), e.g.
    ``PROMPT_CLASSIFICATION_V8``, ``PROMPT_EXTRACTION_V9``.
    ``list_available_versions`` discovers versions by scanning ``os.environ``
    for that prefix — adding a new version means adding a new secret with
    the right name, no code change needed.
"""

import os

_ENV_PREFIX = "PROMPT"


def _env_var_name(prompt_type: str, version: str) -> str:
    return f"{_ENV_PREFIX}_{prompt_type.upper()}_{version.upper()}"


def load_prompt_version(prompt_type: str, version: str) -> str:
    """
    Load a specific version of a prompt from its Infisical-injected env var.

    :param prompt_type: Type of prompt ('classification' or 'extraction').
    :param version: Version string (e.g., 'v1', 'v2', 'v3').
    :returns: Prompt text content.
    :raises FileNotFoundError: If the env var for that version isn't set
        (Infisical secret missing from the deployment's environment).
    :raises ValueError: If prompt_type is invalid.
    """
    if prompt_type not in ["classification", "extraction"]:
        raise ValueError(f"Invalid prompt_type: {prompt_type}. Must be 'classification' or 'extraction'")

    env_var = _env_var_name(prompt_type, version)
    value = os.environ.get(env_var)
    if value is None:
        raise FileNotFoundError(
            f"Prompt version not found: env var {env_var} is not set "
            f"(check the Infisical secret for this deployment's environment)."
        )
    return value.strip()


def list_available_versions(prompt_type: str) -> list[str]:
    """
    List all available versions of a prompt type, by scanning env vars.

    :param prompt_type: Type of prompt ('classification' or 'extraction').
    :returns: List of version strings (e.g., ['v1', 'v2']), sorted.
    """
    prefix = f"{_ENV_PREFIX}_{prompt_type.upper()}_"
    return sorted(key[len(prefix) :].lower() for key in os.environ if key.startswith(prefix))


def get_extraction_prompt(version: str | None = None) -> str:
    """
    Load the NF data extraction prompt (for NFExtractor).

    :param version: Specific version to load (e.g., 'v1', 'v2'). If None,
        loads latest version.
    :returns: Extraction prompt text.
    """
    if version is None:
        # Load latest version
        versions = list_available_versions("extraction")
        version = versions[-1] if versions else "v1"
    return load_prompt_version("extraction", version)


def get_classification_prompt(version: str | None = None) -> str:
    """
    Load the page classification prompt (for GeminiClassifier).

    :param version: Specific version to load (e.g., 'v1', 'v2'). If None,
        loads latest version.
    :returns: Classification prompt text.
    """
    if version is None:
        # Load latest version
        versions = list_available_versions("classification")
        version = versions[-1] if versions else "v1"

    return load_prompt_version("classification", version)


def __getattr__(name: str) -> str:
    """Resolve ``EXTRACTION_PROMPT`` / ``CLASSIFICATION_PROMPT`` lazily.

    Module-level constants would read the environment at import time
    (forbidden by the styleguide); this defers the read to first access
    while keeping the ``from .prompts import EXTRACTION_PROMPT`` call sites
    unchanged.

    :param name: Attribute being accessed.
    :returns: The rendered prompt text.
    :raises AttributeError: For any other attribute name.
    """
    if name == "EXTRACTION_PROMPT":
        return get_extraction_prompt()
    if name == "CLASSIFICATION_PROMPT":
        return get_classification_prompt()
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


# Lazily provided by ``__getattr__`` above (annotation-only: no import-time read).
CLASSIFICATION_PROMPT: str
EXTRACTION_PROMPT: str

__all__ = [
    "CLASSIFICATION_PROMPT",
    "EXTRACTION_PROMPT",
    "get_classification_prompt",
    "get_extraction_prompt",
    "list_available_versions",
    "load_prompt_version",
]
