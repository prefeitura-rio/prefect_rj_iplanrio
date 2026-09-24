"""Prompts de classificação e extração, lidos de variáveis de ambiente ``PROMPT_{TIPO}_{VERSAO}``.

O texto dos prompts não é versionado no repositório; cada versão é um segredo do Infisical.
"""

import os
import re
from dataclasses import dataclass

PROMPT_TYPES = ("classification", "extraction")

HINT_TEMPLATE = (
    "<<<\n"
    "NOTA DE PRÉ-CLASSIFICAÇÃO (inserida automaticamente pelo pipeline):\n"
    "O classificador automático identificou esta página como um possível "
    "documento do tipo **{hint}**. Use como referência inicial, "
    "mas confirme visualmente antes de extrair — a classificação pode estar incorreta.\n"
    ">>>\n\n"
)


@dataclass(frozen=True)
class PromptSet:
    """Versões e textos dos dois prompts usados numa sessão."""

    classification_version: str
    classification_text: str
    extraction_version: str
    extraction_text: str


def env_var_name(prompt_type: str, version: str) -> str:
    """Monta o nome da variável de ambiente de um prompt.

    :param prompt_type: ``"classification"`` ou ``"extraction"``.
    :param version: Versão, ex. ``"v9"``.
    :returns: Ex. ``"PROMPT_EXTRACTION_V9"``.
    """
    return f"PROMPT_{prompt_type.upper()}_{version.upper()}"


def version_sort_key(version: str) -> tuple[int, str]:
    """Ordena ``v2`` antes de ``v10``; versões fora do padrão ``vN`` vêm primeiro.

    :param version: Versão em minúsculas.
    :returns: Chave de ordenação.
    """
    match = re.fullmatch(r"v(\d+)", version)
    return (int(match.group(1)), "") if match else (-1, version)


def list_versions(prompt_type: str) -> list[str]:
    """Lista as versões disponíveis de um tipo de prompt, em ordem crescente.

    :param prompt_type: ``"classification"`` ou ``"extraction"``.
    :returns: Versões em minúsculas, ex. ``["v8", "v9", "v10"]``.
    """
    prefix = f"PROMPT_{prompt_type.upper()}_"
    versions = [key[len(prefix) :].lower() for key in os.environ if key.startswith(prefix)]
    return sorted(versions, key=version_sort_key)


def load_prompt(prompt_type: str, version: str | None) -> tuple[str, str]:
    """Carrega uma versão de prompt (a mais recente se ``version`` for ``None``).

    :param prompt_type: ``"classification"`` ou ``"extraction"``.
    :param version: Versão desejada ou ``None``.
    :returns: ``(versão, texto)``.
    :raises ValueError: Se ``prompt_type`` for inválido.
    :raises RuntimeError: Se não houver versão disponível ou o texto estiver ausente/vazio.
    """
    if prompt_type not in PROMPT_TYPES:
        raise ValueError(f"Tipo de prompt inválido: {prompt_type!r}")
    if version is None:
        versions = list_versions(prompt_type)
        if not versions:
            raise RuntimeError(f"Nenhuma variável PROMPT_{prompt_type.upper()}_V* definida ({prompt_type}).")
        version = versions[-1]
    env_var = env_var_name(prompt_type, version)
    text = os.environ.get(env_var, "").strip()
    if not text:
        raise RuntimeError(f"Prompt ausente ou vazio: {env_var}")
    return version.lower(), text


def load_prompts(classification_version: str | None = None, extraction_version: str | None = None) -> PromptSet:
    """Carrega os prompts de classificação e extração.

    :param classification_version: Versão fixa ou ``None`` para a mais recente.
    :param extraction_version: Versão fixa ou ``None`` para a mais recente.
    :returns: Conjunto de prompts.
    :raises RuntimeError: Se algum prompt estiver indisponível.
    """
    class_version, class_text = load_prompt("classification", classification_version)
    extr_version, extr_text = load_prompt("extraction", extraction_version)
    return PromptSet(class_version, class_text, extr_version, extr_text)


def extraction_prompt_with_hint(template: str, classification_hint: str | None) -> str:
    """Substitui ``{classification_hint}`` no prompt de extração.

    :param template: Texto do prompt de extração.
    :param classification_hint: Categoria vinda da classificação, ou ``None``.
    :returns: Prompt pronto para envio.
    """
    hint = HINT_TEMPLATE.format(hint=classification_hint) if classification_hint else ""
    return template.replace("{classification_hint}", hint)


def __getattr__(name: str) -> str:
    """Mantém ``prompts.CLASSIFICATION_PROMPT``/``EXTRACTION_PROMPT`` para o código antigo até a Task 12.

    :param name: Atributo acessado.
    :returns: Texto da versão mais recente.
    :raises AttributeError: Para qualquer outro nome.
    """
    if name == "CLASSIFICATION_PROMPT":
        return load_prompt("classification", None)[1]
    if name == "EXTRACTION_PROMPT":
        return load_prompt("extraction", None)[1]
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def list_available_versions(prompt_type: str) -> list[str]:
    """Alias temporário de :func:`list_versions` para o código antigo (removido na Task 12).

    :param prompt_type: ``"classification"`` ou ``"extraction"``.
    :returns: Versões disponíveis.
    """
    return list_versions(prompt_type)


def get_classification_prompt(version: str | None = None) -> str:
    """Alias temporário para o código antigo (removido na Task 12).

    :param version: Versão ou ``None``.
    :returns: Texto do prompt.
    """
    return load_prompt("classification", version)[1]


def get_extraction_prompt(version: str | None = None) -> str:
    """Alias temporário para o código antigo (removido na Task 12).

    :param version: Versão ou ``None``.
    :returns: Texto do prompt.
    """
    return load_prompt("extraction", version)[1]


def load_prompt_version(prompt_type: str, version: str) -> str:
    """Alias temporário para o código antigo (removido na Task 12).

    :param prompt_type: ``"classification"`` ou ``"extraction"``.
    :param version: Versão.
    :returns: Texto do prompt.
    """
    return load_prompt(prompt_type, version)[1]
