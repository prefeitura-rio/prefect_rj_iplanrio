"""Leitura da memória do container pelo cgroup: uso, limite e pico, em cgroup v2 ou v1.

O OOM killer do Kubernetes mata o container pelo uso contado no cgroup, e não pela soma que a extração estima, então é
esse número que dá a pressão real de memória do pod. O uso é o *working set* que o kubelet e o cAdvisor também usam: o
total do cgroup menos o cache de arquivo inativo (``inactive_file``), que o kernel devolve sob pressão. Sem descontá-lo,
o cache de página do spool e dos Parquet lidos para o envio encheria o cgroup até perto do limite sem que houvesse
risco, e o gate de memória pausaria a extração à toa.

Fora de um container com cgroup (desenvolvimento local), nada é lido e as funções devolvem ``None``. O diretório raiz é
um parâmetro para os testes apontarem para um cgroup falso.
"""

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

CGROUP_ROOT = Path("/sys/fs/cgroup")
# Sem limite, o cgroup v1 devolve um valor próximo de 2**63 (PAGE_COUNTER_MAX); acima disto é tratado como ilimitado.
UNLIMITED_BYTES = 1 << 60
UNLIMITED_V2 = "max"


@dataclass(frozen=True)
class PodMemory:
    """Memória do container num instante.

    :param used_bytes: Working set: uso do cgroup menos o cache de arquivo inativo.
    :param limit_bytes: Limite de memória do container; ``None`` se não há limite.
    """

    used_bytes: int
    limit_bytes: int | None

    @property
    def ratio(self) -> float | None:
        """Fração do limite em uso; ``None`` sem limite."""
        return self.used_bytes / self.limit_bytes if self.limit_bytes else None


MemoryReader = Callable[[], PodMemory | None]


def _read_text(path: Path) -> str | None:
    try:
        return path.read_text().strip()
    except OSError:
        return None


def _read_int(path: Path) -> int | None:
    text = _read_text(path)
    try:
        return int(text) if text is not None else None
    except ValueError:
        return None


def _read_stat(path: Path, keys: tuple[str, ...]) -> int:
    """Lê de um ``memory.stat`` o primeiro campo de ``keys`` presente; ``0`` se o arquivo ou os campos faltarem."""
    text = _read_text(path)
    if text is None:
        return 0
    fields = {name: value for name, _, value in (line.partition(" ") for line in text.splitlines())}
    for key in keys:
        try:
            return int(fields[key])
        except (KeyError, ValueError):
            continue
    return 0


def _limit(raw: str | None) -> int | None:
    """Converte o conteúdo do arquivo de limite; ``None`` para ilimitado ou ilegível."""
    if raw is None or raw == UNLIMITED_V2:
        return None
    try:
        limit = int(raw)
    except ValueError:
        return None
    return None if limit <= 0 or limit >= UNLIMITED_BYTES else limit


def read_pod_memory(root: Path = CGROUP_ROOT) -> PodMemory | None:
    """Lê o uso e o limite de memória do container.

    :param root: Raiz do cgroup do container (``/sys/fs/cgroup``).
    :returns: Uso e limite; ``None`` se o cgroup não expõe a memória (fora de um container).
    """
    if (usage := _read_int(root / "memory.current")) is not None:
        inactive = _read_stat(root / "memory.stat", ("inactive_file",))
        return PodMemory(max(usage - inactive, 0), _limit(_read_text(root / "memory.max")))
    v1 = root / "memory"
    if (usage := _read_int(v1 / "memory.usage_in_bytes")) is not None:
        inactive = _read_stat(v1 / "memory.stat", ("total_inactive_file", "inactive_file"))
        return PodMemory(max(usage - inactive, 0), _limit(_read_text(v1 / "memory.limit_in_bytes")))
    return None


def read_peak_bytes(root: Path = CGROUP_ROOT) -> int | None:
    """Lê o pico de memória do cgroup desde que ele foi criado, incluindo o cache de arquivo.

    :param root: Raiz do cgroup do container.
    :returns: Bytes do pico (``memory.peak`` no v2, kernel 5.19+; ``memory.max_usage_in_bytes`` no v1); ``None`` se
        indisponível.
    """
    peak = _read_int(root / "memory.peak")
    return peak if peak is not None else _read_int(root / "memory" / "memory.max_usage_in_bytes")
