from pathlib import Path

import pytest

from pipelines.rj_smtr__triagem_recursos.helpers import artifacts


def _clear_prompt_envs(monkeypatch: pytest.MonkeyPatch) -> None:
    """Remove variáveis de ambiente relacionadas a prompt para testes isolados."""
    for base in (
        "PROMPT_SISTEMA_V4",
        "PROMPT_SISTEMA",
        "SMTR_TRIAGEM_PROMPT",
        "RJ_SMTR__TRIAGEM_RECURSOS_PROMPT",
    ):
        for prefix in ("", "SMTR_TRIAGEM__", "SMTR_TRIAGEM_"):
            monkeypatch.delenv(f"{prefix}{base}", raising=False)


def test_carregar_prompt_sistema_prefers_infisical_env_and_unescapes_newlines(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Prioriza PROMPT_SISTEMA_V4 no ambiente e normaliza quebras literais \\n."""
    _clear_prompt_envs(monkeypatch)
    monkeypatch.setattr(artifacts, "PROMPTS_DIR", tmp_path)
    prompt_escapado = "Linha 1 com instrucoes detalhadas de triagem SMTR\\nLinha 2 complementar suficiente."
    monkeypatch.setenv("PROMPT_SISTEMA_V4", prompt_escapado)

    resultado = artifacts.carregar_prompt_sistema(infisical_secret_path="/rj_smtr__triagem_recursos")
    assert resultado == "Linha 1 com instrucoes detalhadas de triagem SMTR\nLinha 2 complementar suficiente."


def test_carregar_prompt_sistema_uses_derived_path_env(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Aceita chave derivada do secret path do Infisical quando PROMPT_SISTEMA_V4 não está definida."""
    _clear_prompt_envs(monkeypatch)
    monkeypatch.setattr(artifacts, "PROMPTS_DIR", tmp_path)
    prompt_longo = "Instrucoes operacionais via chave derivada do secret path com tamanho superior a 50 chars."
    monkeypatch.setenv("RJ_SMTR__TRIAGEM_RECURSOS_PROMPT", prompt_longo)

    resultado = artifacts.carregar_prompt_sistema(infisical_secret_path="/rj_smtr__triagem_recursos")
    assert resultado == prompt_longo


def test_carregar_prompt_sistema_falls_back_to_disk_or_raises(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Lê prompt em disco quando ausente no ambiente e falha explicitamente sem arquivo nem env."""
    _clear_prompt_envs(monkeypatch)
    monkeypatch.setattr(artifacts, "PROMPTS_DIR", tmp_path)

    with pytest.raises(FileNotFoundError, match="Nenhum arquivo de prompt localizado"):
        artifacts.carregar_prompt_sistema(infisical_secret_path="/rj_smtr__triagem_recursos")

    prompt_file = tmp_path / f"{artifacts.VERSAO_PROMPT_PADRAO}.txt"
    prompt_file.write_text("Prompt local de disco valido com comprimento adequado.", encoding="utf-8")

    assert artifacts.carregar_prompt_sistema(is_local=True) == (
        "Prompt local de disco valido com comprimento adequado."
    )
