"""Textos do relatório de paridade: Markdown do artefato do Prefect e linhas do Discord."""

from pipelines.rj_smfp__nota_carioca_paridade_f1.utils.paridade import ParidadeReport

CONTENT_LIMIT = 2000
ELLIPSIS = "…"
MAX_REASONS_IN_DISCORD = 5


def clip(text: str, limit: int = CONTENT_LIMIT) -> str:
    """Corta o texto no limite do Discord, terminando em reticências."""
    return text if len(text) <= limit else text[: limit - len(ELLIPSIS)] + ELLIPSIS


def render_markdown(report: ParidadeReport) -> str:
    """Markdown do artefato: veredito, tabela por regra e o JSON completo.

    :param report: Relatório da paridade.
    :returns: Texto com o JSON num bloco ``json``.
    """
    lines = [
        f"# Paridade F1: {report.resultado}",
        "",
        f"Corte de NSU: {report.corte_nsu if report.corte_nsu is not None else 'n/d'}",
        "",
        "| Regra | Comparados | Iguais | Correções catalogadas | Divergentes |",
        "|---|---:|---:|---|---:|",
    ]
    for item in report.regras:
        corrections = ", ".join(f"{key}: {count}" for key, count in item.correcoes_catalogadas.items()) or "-"
        lines.append(f"| {item.regra} | {item.comparados} | {item.iguais} | {corrections} | {item.divergentes} |")
    if report.motivos:
        lines += ["", "## Motivos", "", *(f"- {motivo}" for motivo in report.motivos)]
    lines += ["", "## Relatório JSON", "", "```json", report.to_json(), "```"]
    return "\n".join(lines)


def approved_line(report: ParidadeReport) -> str:
    """Linha do Discord para a paridade aprovada."""
    compared = sum(item.comparados for item in report.regras)
    corrections = sum(sum(item.correcoes_catalogadas.values()) for item in report.regras)
    return (
        f"✅ **Paridade F1** aprovada: {len(report.regras)} regras, {compared} comparados "
        f"({corrections} em correções catalogadas), corte NSU {report.corte_nsu}"
    )


def rejected_line(report: ParidadeReport) -> str:
    """Linha do Discord para a paridade reprovada, com os primeiros motivos (cada um cita a regra)."""
    shown = report.motivos[:MAX_REASONS_IN_DISCORD]
    more = len(report.motivos) - len(shown)
    suffix = f"; +{more}" if more > 0 else ""
    return f"❌ **Paridade F1** reprovada (corte NSU {report.corte_nsu}): {'; '.join(shown)}{suffix}"


def failed_line(error: Exception) -> str:
    """Linha do Discord para uma falha antes do veredito (dbt, BigQuery, resumo fora do contrato)."""
    return f"❌ **Paridade F1** falhou antes do relatório: `{clip(f'{type(error).__name__}: {error}', 300)}`"
