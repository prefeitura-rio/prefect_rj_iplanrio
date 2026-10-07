"""Orquestração pura de Python das etapas que combinam vários módulos da sonda."""

from collections.abc import Mapping, Sequence

from pipelines.rj_smfp__nota_carioca_oracle_probe.utils.measure import TableBenchmark
from pipelines.rj_smfp__nota_carioca_oracle_probe.utils.profile import TableProfile
from pipelines.rj_smfp__nota_carioca_oracle_probe.utils.rates import TableRates, table_rates
from pipelines.rj_smfp__nota_carioca_oracle_probe.utils.report_summary import table_verdict
from pipelines.rj_smfp__nota_carioca_oracle_probe.utils.scaling import ScalingRun


def summarize(
    profiles: Sequence[TableProfile],
    benchmarks: Sequence[TableBenchmark],
    scalings: Sequence[Sequence[ScalingRun]],
) -> tuple[list[TableRates], Mapping[str, tuple[str, str]]]:
    """Calcula as taxas e o gargalo de cada tabela.

    :param profiles: Perfis, na ordem das tabelas.
    :param benchmarks: Medições, na mesma ordem.
    :param scalings: Execuções de escala, na mesma ordem.
    :returns: Taxas por tabela e gargalo com evidência por tabela.
    """
    triples = list(zip(profiles, benchmarks, scalings, strict=True))
    all_rates = [table_rates(p.table, p.num_rows, b, runs) for p, b, runs in triples]
    verdicts = {p.table: table_verdict(b, runs) for p, b, runs in triples}
    return all_rates, verdicts
