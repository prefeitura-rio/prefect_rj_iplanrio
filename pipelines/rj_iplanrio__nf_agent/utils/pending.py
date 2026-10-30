"""PDFs já processados numa ``versao_processamento`` (tabela ``extracao_pagina``)."""

from collections.abc import Sequence

from google.cloud import bigquery

from .bq import run_query
from .storage import PdfRef

CHUNK_SIZE = 10_000


def list_downloaded_pdfs(view_table: str, mes_envio: str) -> list[PdfRef]:
    """Lista os PDFs já baixados de um mês de envio, onde quer que estejam no bucket.

    A view traz um registro por arquivo, com o último mês em que ele foi enviado e a URI da
    cópia preferida (a da pasta ``mes_envio=``, se houver, ou a mais recente).

    :param view_table: View de PDFs baixados, totalmente qualificada.
    :param mes_envio: Mês de envio, ``YYYY-MM-DD``.
    :returns: PDFs ordenados por nome.
    """
    params = [bigquery.ScalarQueryParameter("mes_envio", "DATE", mes_envio)]
    rows = run_query(__file__, "downloaded_pdfs", view_table, params)
    return [PdfRef(name=row["name"], uri=row["uri"]) for row in rows]


def find_done_pdfs(extracao_pagina_table: str, names: Sequence[str], processing_version: str) -> set[str]:
    """Retorna os PDFs de ``names`` que já têm linhas na versão informada.

    As linhas de um PDF são gravadas num único arquivo, então basta uma linha para contar como feito.

    :param extracao_pagina_table: Tabela ``extracao_pagina`` totalmente qualificada.
    :param names: Nomes candidatos (``nome_arquivo``).
    :param processing_version: ``versao_processamento`` da submissão.
    :returns: Subconjunto já processado.
    """
    done: set[str] = set()
    for start in range(0, len(names), CHUNK_SIZE):
        params = [
            bigquery.ScalarQueryParameter("versao_processamento", "STRING", processing_version),
            bigquery.ArrayQueryParameter("nomes", "STRING", list(names[start : start + CHUNK_SIZE])),
        ]
        rows = run_query(__file__, "done_pdfs", extracao_pagina_table, params)
        done.update(row["nome_arquivo"] for row in rows)
    return done
