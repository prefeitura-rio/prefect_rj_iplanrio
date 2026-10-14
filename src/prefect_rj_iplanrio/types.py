"""Type aliases of the shared Prefect code."""

from typing import Literal

type DumpMode = Literal["append", "overwrite"]
type FileFormat = Literal["parquet", "csv"]
