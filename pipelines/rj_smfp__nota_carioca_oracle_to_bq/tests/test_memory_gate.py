# ruff: noqa: PLR2004
import math
import time

from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.cgroup_memory import PodMemory
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.memory_gate import GateSummary, MemoryGate

MIB = 1024 * 1024
LIMIT = 8192 * MIB


class Meter:
    """Leitor de memória falso: devolve o que o teste mandou."""

    def __init__(self) -> None:
        self.reading: PodMemory | None = None

    def __call__(self) -> PodMemory | None:
        return self.reading

    def at(self, ratio: float, limit: int | None = LIMIT) -> None:
        self.reading = PodMemory(math.ceil(ratio * LIMIT), limit)


def make_gate(ratio: float | None = 0.85) -> tuple[MemoryGate, Meter, list[str]]:
    meter = Meter()
    lines: list[str] = []
    return MemoryGate(ratio, meter, lines.append), meter, lines


def test_gate_admits_while_memory_is_below_the_pause_ratio() -> None:
    # Given a pod at 60% of its limit
    gate, meter, lines = make_gate()
    meter.at(0.60)
    # When the gate samples it
    gate.refresh()
    # Then chunks are admitted and nothing is logged
    assert gate.admit(in_flight=3)
    assert not gate.paused
    assert lines == []


def test_gate_pauses_at_the_ratio_and_logs_once_in_portuguese_with_usage_and_limit() -> None:
    # Given memory that reaches exactly the pause ratio and stays there
    gate, meter, lines = make_gate()
    meter.at(0.85)
    # When the gate samples it several times
    for _ in range(5):
        gate.refresh()
    # Then it is paused, with in-flight chunks still running refused new ones, and logged a single time
    assert gate.paused
    assert not gate.admit(in_flight=2)
    assert len(lines) == 1
    assert "6963 MiB de 8192 MiB (85%)" in lines[0]
    assert "pausando" in lines[0]
    assert gate.summary().pauses == 1


def test_gate_only_resumes_below_the_ratio_minus_the_margin() -> None:
    # Given a gate paused at 90%
    gate, meter, lines = make_gate(0.85)
    meter.at(0.90)
    gate.refresh()
    # When memory falls inside the hysteresis band (80% to 85%)
    meter.at(0.82)
    gate.refresh()
    # Then it is still paused
    assert gate.paused
    assert not gate.admit(in_flight=1)
    # When it falls below 80%
    meter.at(0.79)
    gate.refresh()
    # Then it resumes and says so, with the usage
    assert not gate.paused
    assert gate.admit(in_flight=3)
    assert len(lines) == 2
    assert "retomando" in lines[1]
    assert "(79%)" in lines[1]


def test_gate_does_not_flap_between_the_resume_and_pause_levels() -> None:
    # Given a gate that paused and resumed once
    gate, meter, _ = make_gate(0.85)
    meter.at(0.86)
    gate.refresh()
    meter.at(0.70)
    gate.refresh()
    # When memory climbs to just under the pause ratio
    meter.at(0.84)
    gate.refresh()
    # Then it stays resumed, and only reaching the ratio again pauses a second time
    assert not gate.paused
    meter.at(0.85)
    gate.refresh()
    assert gate.paused
    assert gate.summary().pauses == 2


def test_paused_gate_releases_one_chunk_when_nothing_is_reading_so_the_extraction_cannot_stall() -> None:
    # Given a paused gate with memory still high
    gate, meter, _ = make_gate()
    meter.at(0.95)
    gate.refresh()
    # When no chunk is in flight
    # Then one is admitted (and counted); with one in flight, no more
    assert gate.admit(in_flight=0)
    assert not gate.admit(in_flight=1)
    assert gate.admit(in_flight=0)
    assert gate.summary().forced_chunks == 2
    assert gate.paused


def test_no_limit_never_pauses_but_usage_is_still_tracked() -> None:
    # Given a cgroup with no memory limit ("max")
    gate, meter, lines = make_gate()
    meter.reading = PodMemory(used_bytes=7000 * MIB, limit_bytes=None)
    # When the gate samples it
    gate.refresh()
    # Then it cannot judge pressure and admits, while the usage is available for progress
    assert not gate.paused
    assert gate.admit(in_flight=6)
    assert gate.last == PodMemory(7000 * MIB, None)
    assert gate.summary().peak_used_bytes == 7000 * MIB
    assert lines == []


def test_without_a_cgroup_the_gate_is_disabled() -> None:
    # Given a reader that finds no cgroup (local development)
    gate, _, lines = make_gate()
    # When the gate samples it
    gate.refresh()
    # Then it admits everything and reports no usage
    assert gate.admit(in_flight=6)
    assert gate.last is None
    assert gate.summary().peak_used_bytes is None
    assert lines == []


def test_reading_that_disappears_while_paused_resumes_instead_of_blocking_forever() -> None:
    gate, meter, _ = make_gate()
    meter.at(0.9)
    gate.refresh()
    meter.reading = None
    gate.refresh()

    assert not gate.paused


def test_zero_or_none_ratio_disables_the_pause_even_at_the_limit() -> None:
    for ratio in (0, 0.0, None):
        gate, meter, lines = make_gate(ratio)
        meter.at(0.99)
        gate.refresh()
        assert not gate.paused
        assert gate.admit(in_flight=6)
        assert lines == []


def test_off_gate_reads_nothing() -> None:
    gate = MemoryGate.off()

    gate.refresh()

    assert gate.last is None
    assert gate.admit(in_flight=6)


def test_summary_counts_pauses_peak_and_the_time_of_a_pause_still_open() -> None:
    # Given a gate that paused and resumed once, then paused again and is still paused
    gate, meter, _ = make_gate()
    meter.at(0.90)
    gate.refresh()
    time.sleep(0.03)
    meter.at(0.50)
    gate.refresh()
    first_pause = gate.summary().paused_seconds
    meter.at(0.88)
    gate.refresh()
    time.sleep(0.03)
    # When the summary is taken
    summary = gate.summary()
    # Then both pauses count, the open one with the time elapsed so far, and the peak is the highest sample
    assert first_pause >= 0.03
    assert summary.pauses == 2
    assert summary.paused_seconds >= first_pause + 0.03
    assert summary.peak_used_bytes == math.ceil(0.90 * LIMIT)


def test_summary_text_reports_pauses_and_peaks() -> None:
    summary = GateSummary(pauses=2, paused_seconds=45.4, forced_chunks=3, peak_used_bytes=4 * 1024**3)

    text = summary.describe(cgroup_peak_bytes=5 * 1024**3)

    assert text == (
        "memória do pod: pico de 4.0 GB, memory.peak 5.0 GB (com cache de arquivo); "
        "2 pausas por memória, 45 s no total, 3 faixas liberadas com a memória ainda alta"
    )


def test_summary_text_without_pauses_or_cgroup() -> None:
    quiet = GateSummary(peak_used_bytes=1024**3).describe(cgroup_peak_bytes=None)
    unavailable = GateSummary().describe(cgroup_peak_bytes=None)

    assert quiet == "memória do pod: pico de 1.0 GB; nenhuma pausa por memória"
    assert unavailable.startswith("memória do pod: sem leitura do cgroup")
    assert "nenhuma pausa por memória" in unavailable
