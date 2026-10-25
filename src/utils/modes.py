"""The ways to load a Delta table: what each one reads, skips, writes and cleans."""

from collections.abc import Collection, Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from toolkit.chunks import (
    NULL_CHUNK,
    Chunk,
    LedgerEntry,
    Probe,
    select_pending,
    split_chunks,
)
from toolkit.constants import ALL_KEY
from toolkit.delta import merge_chunk, write_chunk
from toolkit.executor.sources.protocols import Source
from toolkit.functions.native import now_sao_paulo
from toolkit.types import Chunks, MergeSpec, WriteFlags

from .types import DatabaseDumpOptions, DumpMode, DumpOptions, Shard


@dataclass(frozen=True, slots=True)
class Replace:
    """Load by chunks: skip a chunk that did not change, and replace one that did."""

    spec: Chunks | None


@dataclass(frozen=True, slots=True)
class Overwrite:
    """Load by chunks, write every chunk again, and delete the chunks that left the range."""

    spec: Chunks | None


@dataclass(frozen=True, slots=True)
class Capture:
    """Add the rows of one run, in the chunk of its day, and never add the same run twice."""

    at: datetime


@dataclass(frozen=True, slots=True)
class Merge:
    """Match the rows of one run to the table by key, and write the new and changed ones."""

    spec: MergeSpec


type Mode = Replace | Overwrite | Capture | Merge


def check_shard(spec: Chunks | None, shard: Shard | None) -> None:
    """Raise `ValueError` for a shard with no `chunks` to slice, or with an index out of range."""
    if shard is None:
        return

    if spec is None:
        raise ValueError("A shard is a slice of the chunks: set chunks")

    if not 0 <= shard["index"] < shard["count"]:
        raise ValueError(f"The shard {shard['index']} is not in the {shard['count']} shards")


def pick_mode(dump_mode: DumpMode, options: DumpOptions) -> Mode:
    """Return the way to load for a dump mode and its options.

    Raise `ValueError` for the merge mode without the `merge` options or the other way
    round, for a merge with `chunks` or with a `capture`, for a capture with `chunks`
    or with an `overwrite`, and for a `shard` that does not fit (see `check_shard`).
    """
    spec = options.get("chunks")
    capture = options.get("capture")
    merge = options.get("merge")
    check_shard(spec, options.get("shard"))

    if (dump_mode == "merge") != (merge is not None):
        raise ValueError("The merge mode and the merge options go together")

    match dump_mode:
        case "merge" if spec or capture:
            raise ValueError("A merge is one chunk: drop chunks and capture")
        case "merge" if merge is not None:
            return Merge(merge)
        case "overwrite" if capture:
            raise ValueError("A capture is added as one chunk: drop chunks, and do not overwrite")
        case "overwrite":
            return Overwrite(spec)
        case "append" if capture and spec:
            raise ValueError("A capture is added as one chunk: drop chunks, and do not overwrite")
        case "append" if capture:
            return Capture(capture)
        case _:
            return Replace(spec)


def cut_range(spec: Chunks | None) -> list[Chunk]:
    """Return the chunks of a range, then the chunk of the null rows, or `all` with no range."""
    if spec is None:
        return [Chunk(ALL_KEY, 0, 0)]
    return [*split_chunks(spec, now_sao_paulo().date()), NULL_CHUNK]


def cut_mode(mode: Mode) -> list[Chunk]:
    """Return the chunks that a mode loads."""
    match mode:
        case Replace(spec) | Overwrite(spec):
            return cut_range(spec)
        case Capture(at):
            return [Chunk(at.date().isoformat(), 0, 0)]
        case Merge():
            return cut_range(None)


def pick_shard(cut: Sequence[Chunk], shard: Shard) -> list[Chunk]:
    """Return the chunks of one shard: every `count`-th chunk, from `index`.

    The slices of all the shards hold every chunk once, and a dense area of the range
    spreads over the shards. It reads the whole `cut`, never the ledger, so the parent and
    the children agree on it.
    """
    return list(cut[shard["index"] :: shard["count"]])


def plan_shards(dump_mode: DumpMode, options: DatabaseDumpOptions) -> int:
    """Return how many child runs a dump takes: its `shards`, and never more than its chunks.

    Raise `ValueError` for fewer than one shard, for `shards` in a run that is a shard,
    and for a dump that has no `chunks` or that is not a load by chunks.
    """
    shards = options.get("shards", 0)

    if shards < 1:
        raise ValueError("The shards must be at least 1")

    if "shard" in options:
        raise ValueError("A shard cannot have shards of its own")

    if "chunks" not in options:
        raise ValueError("Shards cut the chunks: set chunks")

    return min(shards, len(cut_mode(pick_mode(dump_mode, options))))


def build_ledger_key(mode: Mode, chunk: Chunk) -> str:
    """Return the key of a chunk in the ledger: the time of a capture, or the chunk itself."""
    match mode:
        case Capture(at):
            return at.isoformat()
        case _:
            return chunk.key


def pick_pending(mode: Mode, cut: Sequence[Chunk], seen: Collection[str]) -> list[Chunk]:
    """Return the chunks to read, from the `seen` ones that the ledger knows.

    An append reads the new chunks and the last ones of the `lookback`. A capture reads
    only a run that the ledger does not know. An overwrite and a merge read everything.
    """
    match mode:
        case Overwrite() | Merge():
            return list(cut)
        case Capture():
            return [chunk for chunk in cut if chunk.key not in seen]
        case Replace(spec):
            return select_pending(cut, seen, (spec or {}).get("lookback"))


def probes(mode: Mode) -> bool:
    """Tell whether a chunk can be skipped before its download, with a probe.

    Only the `Replace` mode can: an overwrite writes every chunk again, and a capture and
    a merge load one chunk from a new folder on each run.
    """
    return isinstance(mode, Replace)


def pick_probe(source: Source, mode: Mode, options: DumpOptions) -> Probe | None:
    """Return how a dump probes its chunks, or `None` when it downloads them all.

    A source with a `probe_kind` that needs no column (Oracle, CouchDB) probes unless
    `probe` is false. A database probes when `range_column` is set. `probe: false` and
    a mode that skips no download (see `probes`) give `None`. Raise `ValueError` for
    `probe: true` on a source with no probe, for `probe: true` on a database with no
    `range_column`, and for a `range_column` on a source that does not use one.

    ```python
    pick_probe(postgres, Replace(None), {"range_column": "updated_at"})
    # Probe("pg-range-1", "updated_at")
    ```
    """
    wanted = options.get("probe")
    column = options.get("range_column")

    if wanted and not source.probe_kind:
        raise ValueError("This source has no probe: drop the option probe")

    if column and not source.range_column_needed:
        raise ValueError("The option range_column is for a database probe only")

    if wanted and source.range_column_needed and not column:
        raise ValueError("The probe of this source needs the option range_column")

    if wanted is False or not probes(mode) or not source.probe_kind:
        return None

    if source.range_column_needed and not column:
        return None

    return Probe(source.probe_kind, column)


def skips_unchanged(mode: Mode) -> bool:
    """Tell whether a chunk with the fingerprint of the ledger is left alone."""
    return not isinstance(mode, Overwrite)


def sweeps(mode: Mode) -> bool:
    """Tell whether the chunks outside the range are deleted at the end."""
    return isinstance(mode, Overwrite)


@dataclass(frozen=True, slots=True)
class Loader:
    """The writer of one dump: a mode, the table, and the time stamp of its rows."""

    mode: Mode
    uri: str
    extracted_at: datetime | None

    def write(self, folder: Path, chunk: Chunk, entry: LedgerEntry) -> None:
        """Write the Parquet files of a chunk to the table, as the mode says."""
        at = self.extracted_at

        match self.mode:
            case Replace():
                write_chunk(self.uri, folder, chunk.key, entry, WriteFlags(extracted_at=at))
            case Overwrite():
                flags = WriteFlags(overwrite_schema=True, extracted_at=at)
                write_chunk(self.uri, folder, chunk.key, entry, flags)
            case Capture():
                write_chunk(self.uri, folder, chunk.key, entry, WriteFlags(append=True, extracted_at=at))
            case Merge(spec):
                merge_chunk(self.uri, folder, spec, entry, at)
