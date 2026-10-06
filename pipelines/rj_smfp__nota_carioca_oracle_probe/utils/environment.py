"""Ambiente do pod: CPUs, memória, versões e cgroup."""

import os
import socket
import sys
from dataclasses import dataclass
from pathlib import Path

import oracledb
import pyarrow as pa

CGROUP_V2_CPU = Path("/sys/fs/cgroup/cpu.max")
CGROUP_V2_MEMORY = Path("/sys/fs/cgroup/memory.max")
CGROUP_V1_QUOTA = Path("/sys/fs/cgroup/cpu/cpu.cfs_quota_us")
CGROUP_V1_PERIOD = Path("/sys/fs/cgroup/cpu/cpu.cfs_period_us")
CGROUP_V1_MEMORY = Path("/sys/fs/cgroup/memory/memory.limit_in_bytes")
# O cgroup v1 sem limite devolve um valor próximo de 2**63; qualquer coisa acima disto é "sem limite".
UNLIMITED_MEMORY_BYTES = 2**60
NODE_NAME_VARIABLES = ("NODE_NAME", "KUBERNETES_NODE_NAME", "K8S_NODE_NAME", "MY_NODE_NAME", "SPEC_NODENAME")
MIB = 1024 * 1024


@dataclass(frozen=True)
class EnvironmentInfo:
    """Recursos e versões do pod que roda a sonda.

    :param os_cpus: ``os.cpu_count()``.
    :param affinity_cpus: CPUs permitidas ao processo.
    :param quota_cpus: CPUs da cota do cgroup; ``None`` sem cota.
    :param memory_limit_bytes: Limite de memória do cgroup; ``None`` sem limite.
    :param hostname: Nome do host.
    :param node_name: Nó do Kubernetes, se informado por variável de ambiente.
    :param versions: Versões do Python, python-oracledb, pyarrow e Instant Client.
    """

    os_cpus: int
    affinity_cpus: int
    quota_cpus: float | None
    memory_limit_bytes: int | None
    hostname: str
    node_name: str | None
    versions: dict[str, str]

    @property
    def effective_cpus(self) -> float:
        """Retorna as CPUs efetivas: o menor entre afinidade e cota."""
        return effective_cpus(self.affinity_cpus, self.quota_cpus)

    @property
    def memory_limit_mb(self) -> int | None:
        """Retorna o limite de memória em MiB, ou ``None`` sem limite."""
        return None if self.memory_limit_bytes is None else self.memory_limit_bytes // MIB


def parse_cpu_max(text: str) -> float | None:
    """Lê o ``cpu.max`` do cgroup v2 (``<cota> <período>`` ou ``max <período>``).

    :param text: Conteúdo do arquivo.
    :returns: CPUs da cota; ``None`` se for ``max``.
    """
    quota, period = text.split()[:2]
    return None if quota == "max" else int(quota) / int(period)


def parse_cfs(quota_text: str, period_text: str) -> float | None:
    """Lê a cota do cgroup v1 (``cpu.cfs_quota_us`` e ``cpu.cfs_period_us``).

    :param quota_text: Conteúdo de ``cpu.cfs_quota_us``; ``-1`` significa sem cota.
    :param period_text: Conteúdo de ``cpu.cfs_period_us``.
    :returns: CPUs da cota; ``None`` sem cota.
    """
    quota = int(quota_text.strip())
    return None if quota < 0 else quota / int(period_text.strip())


def parse_memory_limit(text: str) -> int | None:
    """Lê o limite de memória do cgroup (``memory.max`` ou ``memory.limit_in_bytes``).

    :param text: Conteúdo do arquivo.
    :returns: Bytes; ``None`` se for ``max`` ou o valor "ilimitado" do cgroup v1.
    """
    value = text.strip()
    if value == "max":
        return None
    limit = int(value)
    return None if limit >= UNLIMITED_MEMORY_BYTES else limit


def effective_cpus(affinity_cpus: int, quota_cpus: float | None) -> float:
    """Calcula as CPUs efetivas do processo.

    :param affinity_cpus: CPUs permitidas pela afinidade.
    :param quota_cpus: CPUs da cota do cgroup, se houver.
    :returns: O menor dos dois.
    """
    return float(affinity_cpus) if quota_cpus is None else min(float(affinity_cpus), quota_cpus)


def read_text(path: Path) -> str | None:
    """Lê um arquivo do cgroup.

    :param path: Caminho do arquivo.
    :returns: O texto, ou ``None`` se o arquivo não existir nesta versão do cgroup.
    """
    return path.read_text(encoding="utf-8") if path.exists() else None


def read_quota_cpus() -> float | None:
    """Lê a cota de CPU do cgroup v2 ou, na falta dele, do v1.

    :returns: CPUs da cota; ``None`` sem cota ou sem cgroup.
    """
    v2 = read_text(CGROUP_V2_CPU)
    if v2 is not None:
        return parse_cpu_max(v2)
    quota, period = read_text(CGROUP_V1_QUOTA), read_text(CGROUP_V1_PERIOD)
    return None if quota is None or period is None else parse_cfs(quota, period)


def read_memory_limit() -> int | None:
    """Lê o limite de memória do cgroup v2 ou, na falta dele, do v1.

    :returns: Bytes; ``None`` sem limite ou sem cgroup.
    """
    text = read_text(CGROUP_V2_MEMORY)
    if text is None:
        text = read_text(CGROUP_V1_MEMORY)
    return None if text is None else parse_memory_limit(text)


def client_version() -> str:
    """Retorna a versão do Instant Client, iniciando o modo thick se preciso.

    :returns: Versão ou o motivo de não estar disponível.
    """
    try:
        if oracledb.is_thin_mode():
            oracledb.init_oracle_client()
        return ".".join(str(part) for part in oracledb.clientversion())
    except oracledb.Error as error:
        return f"indisponível ({error})"


def describe_environment() -> EnvironmentInfo:
    """Coleta o ambiente do pod.

    :returns: CPUs, memória, host e versões.
    """
    node_name = next((os.environ[name] for name in NODE_NAME_VARIABLES if name in os.environ), None)
    return EnvironmentInfo(
        os_cpus=os.cpu_count() or 0,
        affinity_cpus=len(os.sched_getaffinity(0)),
        quota_cpus=read_quota_cpus(),
        memory_limit_bytes=read_memory_limit(),
        hostname=socket.gethostname(),
        node_name=node_name,
        versions={
            "python": sys.version.split()[0],
            "oracledb": oracledb.__version__,
            "pyarrow": pa.__version__,
            "oracle client": client_version(),
        },
    )
