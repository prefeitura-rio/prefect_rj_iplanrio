"""Text of the Discord lines."""

from pipelines.rj_smfp__nota_carioca_extracao_xml.constants import SECONDS_PER_HOUR, SECONDS_PER_MINUTE


def format_duration(seconds: float) -> str:
    """Write a duration as `12s`, `4min 05s` or `1h 02min`."""
    total = max(int(seconds), 0)
    hours, rest = divmod(total, SECONDS_PER_HOUR)
    minutes, secs = divmod(rest, SECONDS_PER_MINUTE)

    if hours:
        return f"{hours}h {minutes:02d}min"

    return f"{minutes}min {secs:02d}s" if minutes else f"{secs}s"
