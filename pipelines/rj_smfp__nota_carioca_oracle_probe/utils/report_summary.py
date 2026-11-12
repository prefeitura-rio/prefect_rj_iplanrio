"""Seções 6 e 7 do relatório e o resumo em markdown do artefato."""

from collections.abc import Mapping, Sequence

from pipelines.rj_smfp__nota_carioca_oracle_probe.utils.environment import EnvironmentInfo
from pipelines.rj_smfp__nota_carioca_oracle_probe.utils.format import (
    block,
    fmt_duration,
    fmt_gb,
    fmt_int,
    fmt_rows_per_second,
    key_values,
    text_table,
)
from pipelines.rj_smfp__nota_carioca_oracle_probe.utils.measure import TableBenchmark
from pipelines.rj_smfp__nota_carioca_oracle_probe.utils.rates import (
    CPU_BOUND_RATIO,
    SCALING_EFFICIENCY_MIN,
    TableRates,
    bench_totals,
    bottleneck,
    build_scenarios,
    fetch_efficiency,
)
from pipelines.rj_smfp__nota_carioca_oracle_probe.utils.scaling import ScalingRun

ASSUMPTIONS = (
    "Premissas: taxa de um worker = menor execução completa medida (linhas/s ÷ N), escala linear com os workers;\n"
    "o teto do banco (fetch paralelo no maior N) só é aplicado quando a eficiência do fetch ficou abaixo de "
    f"{SCALING_EFFICIENCY_MIN:.0%}.\n"
    "Atual = cada tabela num pod (tempo total = o da maior); hipotéticos = 1 worker por CPU, tabelas em sequência.\n"
    "Linhas totais vêm das estatísticas do dicionário (estimativa); amostra pequena, trate como ordem de grandeza."
)


def format_rates(all_rates: Sequence[TableRates]) -> str:
    """Tabela das taxas e dos volumes usados na extrapolação.

    :param all_rates: Taxas por tabela.
    :returns: Texto.
    """
    rows = [
        [
            r.table,
            fmt_int(r.total_rows),
            "n/d" if r.worker_rate is None else fmt_rows_per_second(r.worker_rate),
            "sem saturação" if r.db_cap is None else fmt_rows_per_second(r.db_cap),
            "n/d" if r.bytes_per_row is None else f"{r.bytes_per_row:,.0f}",
            fmt_gb(None if r.total_rows is None or r.bytes_per_row is None else r.total_rows * r.bytes_per_row),
        ]
        for r in all_rates
    ]
    return text_table(["tabela", "linhas (stats)", "1 worker", "teto do banco", "bytes/linha", "Parquet total"], rows)


def format_extrapolation(all_rates: Sequence[TableRates]) -> str:
    """Formata a seção 6: extrapolação para a carga completa.

    :param all_rates: Taxas por tabela.
    :returns: Bloco de texto.
    """
    tables = [r.table for r in all_rates]
    rows = [
        [s.label, *(fmt_duration(s.seconds_by_table[t]) for t in tables), fmt_duration(s.total_seconds)]
        for s in build_scenarios(all_rates)
    ]
    known = [r.total_rows * r.bytes_per_row for r in all_rates if r.total_rows is not None and r.bytes_per_row]
    total = f"\n\nParquet total estimado: {fmt_gb(sum(known))}" + ("" if len(known) == len(all_rates) else " (parcial)")
    body = f"{format_rates(all_rates)}\n\n{text_table(['cenário', *tables, 'total'], rows)}{total}\n\n{ASSUMPTIONS}"
    return block("6. Extrapolação da carga completa (FULL)", body)


def table_verdict(benchmark: TableBenchmark, runs: Sequence[ScalingRun]) -> tuple[str, str]:
    """Aplica a regra do gargalo a uma tabela.

    :param benchmark: Medição em um processo.
    :param runs: Execuções do teste de escala.
    :returns: Gargalo e evidência; ou aviso se não houve linhas medidas.
    """
    totals = bench_totals(benchmark)
    if not totals.rows:
        return "indeterminado", "nenhuma faixa com dados foi medida"
    return bottleneck(totals, fetch_efficiency(totals, runs))


def format_verdict(
    benchmarks: Sequence[TableBenchmark], scalings: Mapping[str, Sequence[ScalingRun]], environment: EnvironmentInfo
) -> str:
    """Formata a seção 7: gargalo provável.

    :param benchmarks: Medições por tabela.
    :param scalings: Execuções de escala por tabela.
    :param environment: Ambiente do pod.
    :returns: Bloco de texto.
    """
    pairs = []
    for benchmark in benchmarks:
        label, evidence = table_verdict(benchmark, scalings[benchmark.table])
        pairs.append((benchmark.table, f"{label}\n    evidência: {evidence}"))
    rule = (
        "Regra: a etapa de maior tempo por faixa (fetch, conversão, Parquet, envio) é o gargalo. Se for o fetch: "
        f"CPU/relógio > {CPU_BOUND_RATIO} → CPU do cliente; senão eficiência do fetch paralelo < "
        f"{SCALING_EFFICIENCY_MIN} → Oracle/rede saturado; senão latência por conexão (mais workers ajudam)."
    )
    cpus = f"CPUs efetivas do pod: {environment.effective_cpus:.2f}"
    return block("7. Gargalo provável", f"{key_values(pairs)}\n\n{cpus}\n{rule}")


def markdown_summary(all_rates: Sequence[TableRates], verdicts: Mapping[str, tuple[str, str]]) -> str:
    """Resume extrapolação e gargalo em markdown para o artefato do Prefect.

    :param all_rates: Taxas por tabela.
    :param verdicts: Gargalo e evidência por tabela.
    :returns: Markdown com duas tabelas.
    """
    tables = [r.table for r in all_rates]
    lines = ["## Extrapolação da carga completa", "", f"| cenário | {' | '.join(tables)} | total |"]
    lines.append("|" + " --- |" * (len(tables) + 2))
    for s in build_scenarios(all_rates):
        cells = " | ".join(fmt_duration(s.seconds_by_table[t]) for t in tables)
        lines.append(f"| {s.label} | {cells} | {fmt_duration(s.total_seconds)} |")
    lines += ["", "## Gargalo provável", "", "| tabela | gargalo | evidência |", "| --- | --- | --- |"]
    lines += [f"| {t} | {label} | {evidence} |" for t, (label, evidence) in verdicts.items()]
    return "\n".join(lines)
