"""The ways to load a Delta table: what each one reads, skips, writes and cleans."""

from collections.abc import Collection, Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from toolkit.chunks import NULL_CHUNK, Chunk, LedgerEntry, select_pending, split_chunks
from toolkit.constants import ALL_KEY
from toolkit.delta import merge_chunk, write_chunk
from toolkit.functions.native import now_sao_paulo
from toolkit.types import Chunks, MergeSpec, WriteFlags

from .types import DumpMode, DumpOptions


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


def pick_mode(dump_mode: DumpMode, options: DumpOptions) -> Mode:
    """Return the way to load for a dump mode and its options.

    Raise `ValueError` for the merge mode without the `merge` options or the other way
    round, for a merge with `chunks` or with a `capture`, and for a capture with `chunks`
    or with an `overwrite`.
    """
    spec = options.get("chunks")
    capture = options.get("capture")
    merge = options.get("merge")

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
