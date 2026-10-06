"""Seções 1 a 3 do relatório: ambiente do pod, banco e perfil das tabelas."""

from pipelines.rj_smfp__nota_carioca_oracle_probe.utils.database import DatabaseInfo
from pipelines.rj_smfp__nota_carioca_oracle_probe.utils.environment import EnvironmentInfo
from pipelines.rj_smfp__nota_carioca_oracle_probe.utils.format import (
    block,
    fmt_int,
    fmt_mb,
    key_values,
    text_table,
)
from pipelines.rj_smfp__nota_carioca_oracle_probe.utils.profile import TableProfile
from pipelines.rj_smfp__nota_carioca_oracle_probe.utils.softquery import SoftRows

TOP_PARTITIONS = 10
MIB = 1024 * 1024


def format_environment(info: EnvironmentInfo) -> str:
    """Formata a seção 1: ambiente do pod.

    :param info: Ambiente coletado.
    :returns: Bloco de texto.
    """
    quota = "sem cota" if info.quota_cpus is None else f"{info.quota_cpus:.2f}"
    memory = "sem limite" if info.memory_limit_bytes is None else f"{info.memory_limit_bytes / MIB:,.0f} MiB"
    pairs = [
        ("os.cpu_count()", str(info.os_cpus)),
        ("sched_getaffinity", str(info.affinity_cpus)),
        ("cota de CPU do cgroup", quota),
        ("CPUs efetivas", f"{info.effective_cpus:.2f}"),
        ("limite de memória (cgroup)", memory),
        ("hostname", info.hostname),
        ("nó do Kubernetes", info.node_name or "n/d (variável de ambiente ausente)"),
        *info.versions.items(),
    ]
    return block("1. Ambiente do pod", key_values(pairs))


def format_soft(title: str, result: SoftRows) -> str:
    """Formata o resultado de uma consulta fail-soft: tabela, pares ou o motivo da falha.

    :param title: Título da consulta.
    :param result: Resultado.
    :returns: Texto.
    """
    if result.rows is None:
        return f"[{title}]\n  {result.note}"
    if not result.rows:
        return f"[{title}]\n  (sem linhas)"
    if len(result.rows) == 1 and len(result.rows[0]) > 1:
        body = key_values([(key, "n/d" if value is None else str(value)) for key, value in result.rows[0].items()])
    else:
        headers = list(result.rows[0])
        body = text_table(headers, [["n/d" if row[h] is None else str(row[h]) for h in headers] for row in result.rows])
    return f"[{title}]\n" + "\n".join(f"  {line}" for line in body.splitlines())


def format_database(info: DatabaseInfo) -> str:
    """Formata a seção 2: banco de dados.

    :param info: Resultado das consultas ao banco.
    :returns: Bloco de texto.
    """
    parts = [format_soft(title, result) for title, result in info.sections]
    parts.append(f"[Tamanho do bloco]\n  {info.block_size:,} bytes (fonte: {info.block_size_source})")
    return block("2. Banco de dados", "\n\n".join(parts))


def format_partitions(profile: TableProfile) -> str:
    """Resume as partições: totais e as maiores por blocos.

    :param profile: Perfil da tabela.
    :returns: Texto; vazio se não houver partições visíveis.
    """
    if not profile.partitions:
        return "partições: nenhuma visível"
    top = sorted(profile.partitions, key=lambda part: part.blocks or 0, reverse=True)[:TOP_PARTITIONS]
    rows = [[p.name, p.high_value or "-", fmt_int(p.num_rows), fmt_int(p.blocks)] for p in top]
    total_rows = sum(p.num_rows or 0 for p in profile.partitions)
    total_blocks = sum(p.blocks or 0 for p in profile.partitions)
    head = f"partições: {len(profile.partitions)} (soma: {fmt_int(total_rows)} linhas, {fmt_int(total_blocks)} blocos)"
    return f"{head}\ntop {len(top)} por blocos:\n" + text_table(["partição", "high_value", "linhas", "blocos"], rows)


def format_profile(profile: TableProfile, schema: str, chunk_size_blocks: int) -> str:
    """Formata a seção 3 de uma tabela.

    :param profile: Perfil da tabela.
    :param schema: Dono da tabela.
    :param chunk_size_blocks: Tamanho das faixas de ROWID, em blocos.
    :returns: Bloco de texto.
    """
    stats = profile.stats
    pairs: list[tuple[str, str]] = []
    if stats is not None:
        pairs += [
            ("linhas (estatísticas, estimativa)", fmt_int(stats.num_rows)),
            ("blocos", fmt_int(stats.blocks)),
            ("avg_row_len", f"{fmt_int(stats.avg_row_len)} bytes"),
            ("última coleta de estatísticas", stats.last_analyzed or "n/d"),
            ("particionada / grau / compressão", f"{stats.partitioned} / {stats.degree} / {stats.compression or '-'}"),
        ]
    pairs.append((f"tamanho dos segmentos ({profile.segment_source})", fmt_mb(profile.segment_bytes)))
    changes = profile.modifications
    if changes is None:
        pairs.append(("mudanças desde as estatísticas", "n/d"))
    else:
        total = changes.inserts + changes.updates + changes.deletes
        pct = f"{total / stats.num_rows:.2%} das linhas" if stats and stats.num_rows else "taxa n/d"
        pairs.append(
            (
                "mudanças desde as estatísticas",
                f"{fmt_int(changes.inserts)} ins / {fmt_int(changes.updates)} upd / {fmt_int(changes.deletes)} del "
                f"({pct}; despejo do monitoramento: {changes.flushed_at or 'n/d'}, pode estar defasado)",
            )
        )
    kinds = ", ".join(f"{kind}={count}" for kind, count in sorted(profile.kinds.items()))
    pairs += [
        ("colunas", f"{len(profile.columns)} ({kinds})"),
        ("bytes reservados por linha do lote", f"{profile.plan.row_bytes:,}"),
        (
            "lote escolhido (plan_worker_memory)",
            f"{profile.plan.batch_rows:,} linhas, ~{profile.plan.worker_mb:,} MiB/worker",
        ),
        (
            f"faixas de ROWID ({chunk_size_blocks:,} blocos)",
            f"{profile.total_chunks:,} (amostradas: {len(profile.sampled)})",
        ),
    ]
    notes = "".join(f"\nnota: {note}" for note in profile.notes)
    return block(f"3. Tabela {schema}.{profile.table}", f"{key_values(pairs)}\n{format_partitions(profile)}{notes}")
