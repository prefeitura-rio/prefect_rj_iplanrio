"""
Módulo de extração híbrida de texto (Texto Vetorial Nativo + Lazy OCR Fallback).
"""

import io
import logging
import os
from pathlib import Path
import shutil
from typing import Optional, Tuple
import pymupdf

logger = logging.getLogger(__name__)

_TESSERACT_CONFIGURADO = False


def _configurar_tesseract_se_necessario() -> bool:
    """Detecta e configura o caminho do binário tesseract no ambiente (Windows e Linux)."""
    global _TESSERACT_CONFIGURADO
    if _TESSERACT_CONFIGURADO:
        return True

    try:
        import pytesseract
    except ImportError:
        logger.warning("pytesseract não está instalado no ambiente Python.")
        return False

    # 1. Se variável de ambiente explícita estiver definida
    tess_env = os.getenv("TESSERACT_CMD")
    if tess_env and Path(tess_env).exists():
        pytesseract.pytesseract.tesseract_cmd = tess_env
        _TESSERACT_CONFIGURADO = True
        return True

    # 2. Se já estiver no PATH do sistema operacional
    caminho_which = shutil.which("tesseract")
    if caminho_which:
        pytesseract.pytesseract.tesseract_cmd = caminho_which
        _TESSERACT_CONFIGURADO = True
        return True

    # 3. Caminhos padrão do Windows
    candidatos_windows = [
        r"C:\Program Files\Tesseract-OCR\tesseract.exe",
        r"C:\Program Files (x86)\Tesseract-OCR\tesseract.exe",
        Path(os.environ.get("LOCALAPPDATA", "")) / "Programs" / "Tesseract-OCR" / "tesseract.exe",
    ]
    for cand in candidatos_windows:
        cand_path = Path(cand)
        if cand_path.exists():
            pytesseract.pytesseract.tesseract_cmd = str(cand_path)
            _TESSERACT_CONFIGURADO = True
            logger.info("Tesseract localizado automaticamente em: %s", cand_path)
            return True

    # 4. Caminhos padrão do Linux / Docker
    candidatos_linux = [
        "/usr/bin/tesseract",
        "/usr/local/bin/tesseract",
    ]
    for cand in candidatos_linux:
        if Path(cand).exists():
            pytesseract.pytesseract.tesseract_cmd = cand
            _TESSERACT_CONFIGURADO = True
            return True

    logger.warning("Binário do Tesseract OCR não foi localizado no sistema.")
    return False


def _extrair_texto_pagina_fitz(pagina: pymupdf.Page) -> str:
    """Extrai texto vetorial nativo de uma página PyMuPDF com limpeza."""
    try:
        texto = pagina.get_text("text", flags=pymupdf.TEXT_DEHYPHENATE).strip()
    except Exception:
        texto = pagina.get_text("text").strip()

    if len(texto) >= 20:
        return texto

    try:
        blocos = pagina.get_text("blocks")
        linhas_validas = [
            b[4].strip()
            for b in blocos
            if len(b) >= 7 and b[6] == 0 and b[4] and b[4].strip()
        ]
        texto_blocos = "\n".join(linhas_validas).strip()
        if len(texto_blocos) > len(texto):
            return texto_blocos
    except Exception:
        pass

    return texto


def extrair_ocr_pagina_especifica(
    pdf_origem: bytes,
    num_pagina: int,
    dpi: int = 150,
    idioma: str = "por",
    frase_procurada: Optional[str] = None,
) -> Tuple[str, float]:
    """Extrai texto e confiança de uma lauda específica (1-based index).

    Prioriza a camada vetorial nativa do PDF (page.get_text()).
    Se fornecida frase_procurada e a lauda informada não contiver a frase,
    verifica as laudas imediatamente adjacentes (+-1) para compensar desvios
    de numeração (0-based vs 1-based ou carimbo físico de folha).
    Caso seja imagem pura, aciona o Tesseract com tratamento explícito de exceção.

    :param pdf_origem: Bytes do arquivo PDF.
    :param num_pagina: Número da página (1-based index: 1 = primeira página).
    :param dpi: Resolução de renderização caso OCR seja necessário.
    :param idioma: Idioma do OCR (padrão: "por").
    :param frase_procurada: Frase opcional para validação de contexto na lauda.
    :returns: Tupla (texto_extraido, confianca_media).
    """
    if not pdf_origem:
        return "", 0.0

    try:
        doc = pymupdf.open(stream=pdf_origem, filetype="pdf")
        total_pags = len(doc)
        if total_pags == 0 or num_pagina < 1:
            return "", 0.0

        # Converte de 1-based para 0-based
        idx_alvo = min(max(0, num_pagina - 1), total_pags - 1)

        # 1. Tenta extrair a camada nativa da lauda informada
        pagina = doc[idx_alvo]
        texto_nativo = _extrair_texto_pagina_fitz(pagina)

        # Verificação de janela adjacente se fornecida frase procurada
        if frase_procurada and len(frase_procurada.strip()) >= 5:
            alvo_norm = " ".join(frase_procurada.lower().split())
            texto_norm = " ".join(texto_nativo.lower().split())

            if alvo_norm not in texto_norm:
                for offset in [-1, 1]:
                    cand_idx = idx_alvo + offset
                    if 0 <= cand_idx < total_pags:
                        cand_pag = doc[cand_idx]
                        cand_txt = _extrair_texto_pagina_fitz(cand_pag)
                        if alvo_norm in " ".join(cand_txt.lower().split()):
                            logger.info(
                                "Evidência fática localizada na lauda %d (offset de %d).",
                                cand_idx + 1,
                                num_pagina,
                            )
                            doc.close()
                            return cand_txt, 1.0

        if len(texto_nativo) >= 20:
            doc.close()
            return texto_nativo, 1.0

        # 2. Fallback: Página escaneada / Imagem pura -> Tesseract OCR
        tess_disponivel = _configurar_tesseract_se_necessario()
        if not tess_disponivel:
            logger.warning(
                "Tesseract não disponível no ambiente para a página %d. Retornando texto nativo disponível.",
                num_pagina,
            )
            doc.close()
            return texto_nativo, 0.50 if texto_nativo else 0.0

        try:
            from PIL import Image
            import pytesseract

            pix = pagina.get_pixmap(dpi=dpi)
            img = Image.open(io.BytesIO(pix.tobytes("png")))
            dados_ocr = pytesseract.image_to_data(
                img, lang=idioma, output_type=pytesseract.Output.DICT
            )

            confs = [
                float(c)
                for c in dados_ocr.get("conf", [])
                if str(c).strip() not in ["-1", ""]
            ]
            media_conf = (sum(confs) / len(confs) / 100.0) if confs else 0.0
            texto_ocr = " ".join(
                [t for t in dados_ocr.get("text", []) if t.strip()]
            )

            doc.close()
            if texto_ocr.strip():
                return texto_ocr, round(media_conf, 4)
            return texto_nativo, 0.50 if texto_nativo else 0.0

        except Exception as exc:
            logger.warning(
                "Falha ao executar OCR na página %d: %s. Retornando texto nativo disponível.",
                num_pagina,
                exc,
            )
            doc.close()
            return texto_nativo, 0.50 if texto_nativo else 0.0

    except Exception as exc:
        logger.error("Erro ao abrir ou processar PDF para OCR: %s", exc)
        return "", 0.0
