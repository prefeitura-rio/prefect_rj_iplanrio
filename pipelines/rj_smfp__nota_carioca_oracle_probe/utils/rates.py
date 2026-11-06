"""Taxas, extrapolação do tempo de carga completa e regra do gargalo, sem I/O."""

from collections.abc import Sequence
from dataclasses import dataclass

from pipelines.rj_smfp__nota_carioca_oracle_probe.utils.measure import TableBenchmark
from pipelines.rj_smfp__nota_carioca_oracle_probe.utils.scaling import ScalingRun

CPU_BOUND_RATIO = 0.8
"""Fetch com CPU/relógio acima disto é limitado pela CPU do cliente."""
SCALING_EFFICIENCY_MIN = 0.8
"""Eficiência do fetch paralelo abaixo disto indica saturação do banco ou da rede."""
CURRENT_WORKERS_PER_POD = 2
CURRENT_PODS = 3
HYPOTHETICAL_CPUS = (4, 8, 16, 32)


@dataclass(frozen=True)
class BenchTotals:
    """Somas das faixas não vazias medidas em um processo.

    :param rows: Linhas.
    :param fetch_wall: Relógio da leitura.
    :param fetch_cpu: CPU da leitura.
    :param convert_wall: Relógio da conversão.
    :param write_wall: Relógio do Parquet com a compressão principal.
    :param upload_wall: Relógio do envio ao GCS.
    :param fetched_bytes: Bytes Arrow lidos.
    :param converted_bytes: Bytes Arrow convertidos.
    :param parquet_bytes: Bytes do Parquet com a compressão principal.
    """

    rows: int
    fetch_wall: float
    fetch_cpu: float
    convert_wall: float
    write_wall: float
    upload_wall: float
    fetched_bytes: int
    converted_bytes: int
    parquet_bytes: int


@dataclass(frozen=True)
class TableRates:
    """Taxas medidas de uma tabela, base da extrapolação.

    :param table: Nome da tabela.
    :param total_rows: Linhas pelas estatísticas; ``None`` se desconhecido.
    :param bytes_per_row: Bytes de Parquet por linha; ``None`` sem medição.
    :param worker_rate: Linhas/s de um worker no caminho completo; ``None`` sem medição.
    :param db_cap: Linhas/s máximas do banco (fetch paralelo) se houve saturação; ``None`` se não.
    """

    table: str
    total_rows: int | None
    bytes_per_row: float | None
    worker_rate: float | None
    db_cap: float | None


@dataclass(frozen=True)
class Scenario:
    """Tempo estimado da carga completa em um cenário.

    :param label: Descrição do cenário.
    :param seconds_by_table: Segundos por tabela; ``None`` se não estimável.
    :param total_seconds: Tempo total do cenário; ``None`` se faltou alguma tabela.
    """

    label: str
    seconds_by_table: dict[str, float | None]
    total_seconds: float | None


def rate(amount: float, seconds: float) -> float:
    """Divide quantidade por tempo.

    :param amount: Linhas ou bytes.
    :param seconds: Tempo, em segundos.
    :returns: A taxa; zero se o tempo for zero.
    """
    return amount / seconds if seconds > 0 else 0.0


def bench_totals(benchmark: TableBenchmark) -> BenchTotals:
    """Soma as etapas das faixas não vazias.

    :param benchmark: Medição de uma tabela.
    :returns: Totais.
    """
    chunks = [chunk for chunk in benchmark.chunks if chunk.rows]
    return BenchTotals(
        rows=sum(chunk.rows for chunk in chunks),
        fetch_wall=sum(chunk.fetch.wall_seconds for chunk in chunks),
        fetch_cpu=sum(chunk.fetch.cpu_seconds for chunk in chunks),
        convert_wall=sum(chunk.convert.wall_seconds for chunk in chunks),
        write_wall=sum(chunk.writes[0].timing.wall_seconds for chunk in chunks),
        upload_wall=sum(chunk.upload_seconds or 0.0 for chunk in chunks),
        fetched_bytes=sum(chunk.fetched_bytes for chunk in chunks),
        converted_bytes=sum(chunk.converted_bytes for chunk in chunks),
        parquet_bytes=sum(chunk.writes[0].file_bytes for chunk in chunks),
    )


def completed_runs(runs: Sequence[ScalingRun], mode: str) -> list[ScalingRun]:
    """Filtra as execuções que rodaram e leram linhas, em ordem crescente de workers.

    :param runs: Execuções do teste de escala.
    :param mode: ``completo`` ou ``só fetch``.
    :returns: Execuções válidas do modo.
    """
    valid = [run for run in runs if run.mode == mode and run.skipped is None and run.rows and run.work_seconds > 0]
    return sorted(valid, key=lambda run: run.workers)


def fetch_efficiency(totals: BenchTotals, runs: Sequence[ScalingRun]) -> float | None:
    """Mede quanto o fetch paralelo escala: taxa paralela ÷ (N x taxa de um processo).

    :param totals: Totais do processo único.
    :param runs: Execuções do teste de escala.
    :returns: Eficiência (1,0 = linear); ``None`` sem fetch paralelo medido.
    """
    parallel = completed_runs(runs, "só fetch")
    single = rate(totals.rows, totals.fetch_wall)
    if not parallel or single == 0:
        return None
    best = parallel[-1]
    return rate(best.rows, best.work_seconds) / (best.workers * single)


def table_rates(
    table: str, total_rows: int | None, benchmark: TableBenchmark, runs: Sequence[ScalingRun]
) -> TableRates:
    """Resume as taxas que alimentam a extrapolação.

    O worker usa a menor execução completa (menos contenção). O teto do banco só vale se o fetch
    paralelo escalou abaixo de ``SCALING_EFFICIENCY_MIN``.

    :param table: Nome da tabela.
    :param total_rows: Linhas pelas estatísticas.
    :param benchmark: Medição em um processo.
    :param runs: Execuções do teste de escala.
    :returns: Taxas da tabela.
    """
    totals = bench_totals(benchmark)
    full = completed_runs(runs, "completo")
    worker_rate = rate(full[0].rows, full[0].work_seconds) / full[0].workers if full else None
    efficiency = fetch_efficiency(totals, runs)
    cap = None
    if efficiency is not None and efficiency < SCALING_EFFICIENCY_MIN:
        best = completed_runs(runs, "só fetch")[-1]
        cap = rate(best.rows, best.work_seconds)
    return TableRates(
        table=table,
        total_rows=total_rows,
        bytes_per_row=totals.parquet_bytes / totals.rows if totals.rows else None,
        worker_rate=worker_rate,
        db_cap=cap,
    )


def estimate_seconds(rates: TableRates, workers: int) -> float | None:
    """Estima o tempo de extrair a tabela com ``workers`` processos.

    :param rates: Taxas da tabela.
    :param workers: Processos de leitura.
    :returns: Segundos; ``None`` se faltarem linhas ou taxa medida.
    """
    if rates.total_rows is None or not rates.worker_rate:
        return None
    effective = workers * rates.worker_rate
    if rates.db_cap is not None:
        effective = min(effective, rates.db_cap)
    return rates.total_rows / effective


def build_scenarios(all_rates: Sequence[TableRates]) -> list[Scenario]:
    """Monta os cenários: atual (3 pods em paralelo) e hipotéticos de 4 a 32 CPUs.

    Atual: cada tabela num pod, ``CURRENT_WORKERS_PER_POD`` workers, tempo total = o maior. Hipotéticos: um
    worker por CPU, escala linear, tabelas em sequência, tempo total = soma.

    :param all_rates: Taxas de todas as tabelas.
    :returns: Cenários na ordem do relatório.
    """
    current = {r.table: estimate_seconds(r, CURRENT_WORKERS_PER_POD) for r in all_rates}
    known = [seconds for seconds in current.values() if seconds is not None]
    scenarios = [
        Scenario(
            f"atual: {CURRENT_PODS} pods em paralelo x {CURRENT_WORKERS_PER_POD} workers",
            current,
            max(known) if len(known) == len(current) else None,
        )
    ]
    for cpus in HYPOTHETICAL_CPUS:
        by_table = {r.table: estimate_seconds(r, cpus) for r in all_rates}
        known = [seconds for seconds in by_table.values() if seconds is not None]
        scenarios.append(
            Scenario(
                f"{cpus} CPUs, 1 worker por CPU, tabelas em sequência",
                by_table,
                sum(known) if len(known) == len(by_table) else None,
            )
        )
    return scenarios


def bottleneck(totals: BenchTotals, efficiency: float | None) -> tuple[str, str]:
    """Aplica a regra do gargalo ao processo único e ao fetch paralelo.

    Regra: a etapa de maior tempo por linha (fetch, conversão, Parquet, envio) é o gargalo. Se for o fetch:
    CPU/relógio > ``CPU_BOUND_RATIO`` indica CPU do cliente; senão, eficiência do fetch paralelo abaixo de
    ``SCALING_EFFICIENCY_MIN`` indica saturação do Oracle/rede, e acima dela, latência por conexão (mais
    workers resolvem).

    :param totals: Totais do processo único.
    :param efficiency: Eficiência do fetch paralelo; ``None`` se não medida.
    :returns: O gargalo provável e a evidência.
    """
    stages = {
        "fetch": totals.fetch_wall,
        "convert": totals.convert_wall,
        "parquet": totals.write_wall,
        "upload": totals.upload_wall,
    }
    dominant = max(stages, key=lambda name: stages[name])
    total = sum(stages.values())
    share = rate(stages[dominant], total)
    evidence = f"etapa dominante: {dominant} ({share:.0%} do tempo por faixa)"
    if dominant == "fetch":
        ratio = rate(totals.fetch_cpu, totals.fetch_wall)
        evidence += f"; CPU/relógio do fetch = {ratio:.2f}"
        if ratio > CPU_BOUND_RATIO:
            return "CPU do cliente na leitura (driver/OCI)", evidence
        if efficiency is None:
            return "Espera pelo Oracle/rede (fetch com baixa CPU); escala não medida", evidence
        evidence += f"; eficiência do fetch paralelo = {efficiency:.2f}"
        if efficiency < SCALING_EFFICIENCY_MIN:
            return "Capacidade do Oracle/rede (o fetch paralelo saturou)", evidence
        return "Latência do Oracle/rede por conexão (escala com mais workers)", evidence
    labels = {
        "convert": "CPU do cliente na conversão (convert.py)",
        "parquet": "CPU do cliente na gravação/compressão do Parquet",
        "upload": "Upload para o GCS",
    }
    return labels[dominant], evidence
