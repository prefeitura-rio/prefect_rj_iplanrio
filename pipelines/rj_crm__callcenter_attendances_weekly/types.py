"""Types of rj_crm__callcenter_attendances_weekly."""

from typing import TypedDict

from utils.types import DumpMode


class Options(TypedDict, total=False):
    """The options of a run. A key that is not set takes its default.

    `dump_mode` is `append`. The window is `start_date` to `end_date`, or the last
    `date_interval` days (7) when a date is missing. `transcribe_audio` is `True`.
    """

    dump_mode: DumpMode
    start_date: str
    end_date: str
    date_interval: int
    transcribe_audio: bool
