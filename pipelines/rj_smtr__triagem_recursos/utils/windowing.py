"""Gerenciamento de contexto e truncamento inteligente de laudas (Smart Windowing).

Descrição Técnica & Arquitetura:
Executa o truncamento e isolamento seletivo das primeiras laudas de processos
administrativos diretamente em memória RAM utilizando PyMuPDF (`fitz`). Em recursos
de multas da SMTR, o núcleo fático da petição recursal inicial encontra-se invariavelmente
nas primeiras páginas (tipicamente até 20 laudas), enquanto o restante do volume é composto
por espelhos de sistema repetidos, comprovantes de residência e fotos de veículos que
inflam custos de token e introduzem ruído adversarial. O Smart Windowing elimina esses anexos
pesados antes da conversão para imagens e envio aos modelos multimodais de linguagem.

Fluxo de Dependências:
- Consumidores Diretos:
  * `utils/triage.py`: consome `extrair_smart_window_pdf` imediatamente antes da chamada
    primária N=1 e da eventual chamada adaptativa N=2.
- Dependências Diretas:
  * PyMuPDF (`fitz`): biblioteca de baixo nível em C para manipulação in-memory de PDFs.
"""

import io
from pathlib import Path
import pymupdf as fitz


def extrair_smart_window_pdf(
    pdf_origem: Path | str | bytes | io.BytesIO,
    max_paginas: int = 20,
) -> bytes:
    """Trunca um arquivo PDF até o limite estipulado de páginas operando em memória.

    :param pdf_origem: Caminho local (Path/str) OU stream binário (bytes/BytesIO).
    :param max_paginas: Quantidade máxima de páginas a reter (padrão: 20).
    :returns: Binário do sub-PDF truncado e compactado.
    :raises FileNotFoundError: Se o caminho do PDF não existir em disco.
    :raises TypeError: Se o tipo do parâmetro pdf_origem for inválido.
    """
    if isinstance(pdf_origem, bytes):
        doc_origem = fitz.open(stream=pdf_origem, filetype="pdf")
    elif isinstance(pdf_origem, io.BytesIO):
        doc_origem = fitz.open(stream=pdf_origem.getvalue(), filetype="pdf")
    elif isinstance(pdf_origem, (Path, str)):
        caminho = Path(pdf_origem)
        if not caminho.exists():
            raise FileNotFoundError(f"Arquivo não encontrado: {caminho}")
        doc_origem = fitz.open(str(caminho))
    else:
        raise TypeError(f"Tipo de entrada não suportado: {type(pdf_origem)}")

    doc_truncado = fitz.open()

    total_pags = min(len(doc_origem), max_paginas)
    for i in range(total_pags):
        doc_truncado.insert_pdf(doc_origem, from_page=i, to_page=i)

    doc_origem.close()

    pdf_bytes = doc_truncado.tobytes(garbage=4, deflate=True)
    doc_truncado.close()

    return pdf_bytes
