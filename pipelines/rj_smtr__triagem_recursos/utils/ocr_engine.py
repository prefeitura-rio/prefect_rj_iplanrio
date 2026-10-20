"""
Módulo de Reconhecimento Óptico de Caracteres Pontual (Lazy OCR).

Executa a extração de texto e confiança estatística via Tesseract OCR sob demanda
exclusivamente na página física indicada, minimizando consumo computacional.
"""
"""Módulo de extração pontual de OCR via Tesseract."""

from typing import Tuple

def extrair_ocr_pagina_especifica(
    pdf_origem: bytes,
    num_pagina: int,
    dpi: int = 150,
    idioma: str = "por",
) -> Tuple[str, float]:
  """Extrai texto e confiança média de uma página específica.

  Retorna sempre uma tupla (texto, confianca), nunca None.
  """
  try:
    import pymupdf as fitz
    import pytesseract
    from PIL import Image
    import io

    doc = fitz.open(stream=pdf_origem, filetype="pdf")
    if num_pagina < 1 or num_pagina > len(doc):
      return "", 0.0

    pagina = doc[num_pagina - 1]
    pix = pagina.get_pixmap(dpi=dpi)
    img = Image.open(io.BytesIO(pix.tobytes("png")))

    # Executa OCR com métricas de dados
    dados_ocr = pytesseract.image_to_data(
        img, lang=idioma, output_type=pytesseract.Output.DICT
    )
    confs = [
        float(c)
        for c in dados_ocr.get("conf", [])
        if str(c).strip() not in ["-1", ""]
    ]
    media_conf = (sum(confs) / len(confs) / 100.0) if confs else 0.0
    texto_extraido = " ".join(
        [t for t in dados_ocr.get("text", []) if t.strip()]
    )

    return texto_extraido, round(media_conf, 4)

  except Exception:
    # Fallback seguro para ambientes sem Tesseract no PATH
    return "", 0.0