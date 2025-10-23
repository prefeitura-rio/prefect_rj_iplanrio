# -*- coding: utf-8 -*-
"""Query processors for pipeline rj_smas__disparo_pic_lembrete."""

from datetime import datetime, timedelta

from iplanrio.pipelines_utils.logging import log

from pipelines.rj_smas__disparo_pic.constants import PicLembreteConstants


def process_pic_lembrete_query(query: str | None = None) -> str:
    """
    Substitui o placeholder {data_evento} pela data D+2 e retorna a query formatada.

    Args:
        query: Query opcional contendo o placeholder {data_evento}. Se None, usa a default.

    Returns:
        Query formatada com a data alvo no padrão YYYY-MM-DD.

    Raises:
        ValueError: Caso o placeholder {data_evento} não esteja presente.
    """
    final_query = query or PicLembreteConstants.PIC_LEMBRETE_QUERY.value

    if "{data_evento}" not in final_query:
        raise ValueError("Query must contain {data_evento} placeholder for dynamic substitution")

    data_evento = (datetime.now() + timedelta(days=2)).strftime("%Y-%m-%d")
    log(f"PIC_LEMBRETE: Calculated D+2 event date -> {data_evento}")

    return final_query.format(data_evento=data_evento)


QUERY_PROCESSORS = {
    "pic_lembrete": process_pic_lembrete_query,
}


def get_query_processor(processor_name: str):
    """Retorna a função processadora registrada pelo nome informado."""
    return QUERY_PROCESSORS.get(processor_name)
