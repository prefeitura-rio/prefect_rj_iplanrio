"""
Prompts module - Contains all prompts used by the NF processing pipeline.

Prompts are stored as separate .txt files for easy editing and version control.

Versioning:
    Prompts are versioned in `versions/` subdirectories using simple numbering:
    - v1.txt, v2.txt, v3.txt, etc.

    Active prompts (classification_prompt.txt, extraction_prompt.txt) point to
    current versions being used in production.

    Each version has a corresponding CHANGELOG.md documenting changes.
"""

from pathlib import Path

PROMPTS_DIR = Path(__file__).parent
VERSIONS_DIR = PROMPTS_DIR / "versions"


def load_prompt_version(prompt_type: str, version: str) -> str:
    """
    Load a specific version of a prompt.

    :param prompt_type: Type of prompt ('classification' or 'extraction').
    :param version: Version string (e.g., 'v1', 'v2', 'v3'). Note: Legacy
        format 'v1.0.0' also supported for compatibility.
    :returns: Prompt text content.
    :raises FileNotFoundError: If versioned prompt file doesn't exist.
    :raises ValueError: If prompt_type is invalid.
    """
    if prompt_type not in ["classification", "extraction"]:
        raise ValueError(f"Invalid prompt_type: {prompt_type}. Must be 'classification' or 'extraction'")

    version_file = VERSIONS_DIR / prompt_type / f"{version}.txt"
    if not version_file.exists():
        raise FileNotFoundError(f"Prompt version not found: {version_file}")

    with open(version_file, "r", encoding="utf-8") as f:
        return f.read().strip()


def list_available_versions(prompt_type: str) -> list[str]:
    """
    List all available versions of a prompt type.

    :param prompt_type: Type of prompt ('classification' or 'extraction').
    :returns: List of version strings (e.g., ['v1.0.0', 'v1.1.0']).
    """
    versions_path = VERSIONS_DIR / prompt_type
    if not versions_path.exists():
        return []

    return sorted([f.stem for f in versions_path.glob("v*.txt")])


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


# Pre-load prompts for convenience (using latest versions)
EXTRACTION_PROMPT = get_extraction_prompt()
CLASSIFICATION_PROMPT = get_classification_prompt()

__all__ = [
    "CLASSIFICATION_PROMPT",
    "EXTRACTION_PROMPT",
    "PROMPTS_DIR",
    "VERSIONS_DIR",
    "get_classification_prompt",
    "get_extraction_prompt",
    "list_available_versions",
    "load_prompt_version",
]
