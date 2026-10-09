"""Divisão de PDFs em páginas avulsas."""

import base64
from dataclasses import dataclass

import fitz


@dataclass(frozen=True)
class PdfPages:
    """Um PDF de uma sessão e seu total de páginas."""

    name: str
    pages: int


def split_pdf_pages(pdf_bytes: bytes) -> list[str]:
    """Divide um PDF em PDFs de uma página, codificados em base64.

    :param pdf_bytes: Conteúdo do PDF.
    :returns: Uma string base64 por página, na ordem original.
    :raises ValueError: Se o PDF não puder ser lido ou não tiver páginas.
    """
    try:
        source = fitz.open(stream=pdf_bytes, filetype="pdf")
    except Exception as exc:  # PyMuPDF levanta tipos diferentes conforme a versão
        raise ValueError(f"PDF ilegível: {exc}") from exc
    try:
        if source.page_count == 0:
            raise ValueError("PDF ilegível: nenhuma página.")
        pages = []
        for index in range(source.page_count):
            single = fitz.open()
            try:
                single.insert_pdf(source, from_page=index, to_page=index)
                pages.append(base64.b64encode(single.tobytes()).decode("ascii"))
            except RuntimeError as exc:
                raise ValueError(f"PDF ilegível na página {index + 1}: {exc}") from exc
            finally:
                single.close()
        return pages
    finally:
        source.close()
