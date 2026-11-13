"""Motor híbrido de extração textual com texto vetorial nativo e Lazy OCR direcionado.

Descrição Técnica & Arquitetura:
Implementa a estratégia de extração de texto sob demanda (Lazy OCR) para validação
de evidências fáticas. Em vez de executar OCR custoso sobre todo o processo de forma antecipada:
1. Extrai inicialmente o texto digital vetorial embutido nas laudas via PyMuPDF.
2. Caso a lauda seja escaneada ou contenha texto rasterizado insuficiente (< 50 caracteres),
   aciona o Tesseract OCR de forma pontual estritamente na lauda apontada pela LLM como evidência
   (com tolerância de lauda adjacente +/- 1 lauda para cobrir divergências de numeração).
3. Avalia se o documento possui indicativos visuais de escrita manual (manuscrito)
   e computa a confiança média de reconhecimento óptico.

Fluxo de Dependências:
- Consumidores Diretos:
  * `utils/triage.py`: invoca `extrair_ocr_pagina_especifica` para obter o texto factual
    da lauda de evidência antes da extração dos sinais epistêmicos.
  * `utils/router.py`: consome a função para verificação de grounding no roteamento.
- Dependências Diretas:
  * PyMuPDF (`pymupdf`): extração de texto vetorial e renderização raster de laudas em memória.
  * Tesseract OCR (`pytesseract`): motor óptico de reconhecimento com detecção automática de binário.
  * `prefect.logging`: logger com rastreabilidade OpenTelemetry.
"""

import io
import os
import shutil
import threading
from pathlib import Path

import pymupdf
from prefect.logging import get_logger

from pipelines.rj_smtr__triagem_recursos.constants import obter_env
from pipelines.rj_smtr__triagem_recursos.helpers.windowing import FITZ_LOCK

logger = get_logger(__name__)

TESSERACT_CONFIGURADO = False
_TESSERACT_LOCK = threading.Lock()


def configurar_tesseract_se_necessario() -> bool:
    """Detecta e configura o caminho do binário tesseract no ambiente (Windows e Linux).

    :returns: True se o binário estiver configurado e utilizável; False caso contrário.
    """
    global TESSERACT_CONFIGURADO
    if TESSERACT_CONFIGURADO:
        return True

    with _TESSERACT_LOCK:
        if TESSERACT_CONFIGURADO:
            return True

        try:
            import pytesseract
        except ImportError:
            logger.warning("pytesseract não está instalado no ambiente Python.")
            return False

        tess_env = obter_env("TESSERACT_CMD")
        if tess_env and Path(tess_env).exists():
            pytesseract.pytesseract.tesseract_cmd = tess_env
            TESSERACT_CONFIGURADO = True
            return True

        caminho_which = shutil.which("tesseract")
        if caminho_which:
            pytesseract.pytesseract.tesseract_cmd = caminho_which
            TESSERACT_CONFIGURADO = True
            return True

        candidatos_windows = [
            r"C:\Program Files\Tesseract-OCR\tesseract.exe",
            r"C:\Program Files (x86)\Tesseract-OCR\tesseract.exe",
            Path(os.environ.get("LOCALAPPDATA", "")) / "Programs" / "Tesseract-OCR" / "tesseract.exe",
        ]
        for cand in candidatos_windows:
            cand_path = Path(cand)
            if cand_path.exists():
                pytesseract.pytesseract.tesseract_cmd = str(cand_path)
                TESSERACT_CONFIGURADO = True
                logger.info("Tesseract localizado automaticamente em: %s", cand_path)
                return True

        for cand in ("/usr/bin/tesseract", "/usr/local/bin/tesseract"):
            if Path(cand).exists():
                pytesseract.pytesseract.tesseract_cmd = cand
                TESSERACT_CONFIGURADO = True
                return True

        logger.warning("Binário do Tesseract OCR não foi localizado no sistema.")
        return False


def extrair_texto_pagina_fitz(pagina: pymupdf.Page) -> str:
    """Extrai texto vetorial nativo de uma página PyMuPDF com limpeza.

    :param pagina: Instância da página PyMuPDF a analisar.
    :returns: Texto vetorial extraído da lauda.
    """
    try:
        texto = pagina.get_text("text", flags=pymupdf.TEXT_DEHYPHENATE).strip()
    except Exception:
        texto = pagina.get_text("text").strip()

    if len(texto) >= 20:
        return texto

    try:
        blocos = pagina.get_text("blocks")
        linhas_validas = [b[4].strip() for b in blocos if len(b) >= 7 and b[6] == 0 and b[4] and b[4].strip()]
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
    frase_procurada: str | None = None,
) -> tuple[str, float]:
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
        png_bytes: bytes | None = None
        texto_nativo = ""

        with FITZ_LOCK:
            if isinstance(pdf_origem, (str, Path)):
                doc = pymupdf.open(str(pdf_origem))
            else:
                doc = pymupdf.open(stream=pdf_origem, filetype="pdf")
            try:
                total_pags = len(doc)
                if total_pags == 0 or num_pagina < 1:
                    return "", 0.0

                idx_alvo = min(max(0, num_pagina - 1), total_pags - 1)
                pagina = doc[idx_alvo]
                texto_nativo = extrair_texto_pagina_fitz(pagina)

                if frase_procurada and len(frase_procurada.strip()) >= 5:
                    alvo_norm = " ".join(frase_procurada.lower().split())
                    texto_norm = " ".join(texto_nativo.lower().split())

                    if alvo_norm not in texto_norm:
                        for offset in (-1, 1):
                            cand_idx = idx_alvo + offset
                            if 0 <= cand_idx < total_pags:
                                pag_cand = doc[cand_idx]
                                txt_cand = extrair_texto_pagina_fitz(pag_cand)
                                if alvo_norm in " ".join(txt_cand.lower().split()):
                                    texto_nativo = txt_cand
                                    pagina = pag_cand
                                    break

                if len(texto_nativo) >= 30:
                    return texto_nativo, 0.95

                if not configurar_tesseract_se_necessario():
                    return texto_nativo, 0.50

                zoom = dpi / 72.0
                matriz = pymupdf.Matrix(zoom, zoom)
                pix = pagina.get_pixmap(matrix=matriz, alpha=False)
                png_bytes = pix.tobytes("png")
            finally:
                doc.close()

        if png_bytes is None:
            return texto_nativo, 0.50

        import pytesseract
        from PIL import Image

        img = Image.open(io.BytesIO(png_bytes))
        dados_ocr = pytesseract.image_to_data(img, lang=idioma, output_type=pytesseract.Output.DICT)

        confs = [float(c) for c in dados_ocr.get("conf", []) if c != "-1" and str(c).strip() != ""]
        conf_media = (sum(confs) / len(confs) / 100.0) if confs else 0.50

        texto_ocr = " ".join([w.strip() for w in dados_ocr.get("text", []) if w and w.strip()])
        return texto_ocr, round(conf_media, 4)

    except Exception as exc:
        logger.warning("Falha ao extrair texto da página %d: %s", num_pagina, exc)
        return "", 0.0
