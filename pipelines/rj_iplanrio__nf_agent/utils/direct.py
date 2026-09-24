"""Chamadas diretas por página ao Bifrost (sem batch), para rodar poucos PDFs localmente."""

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass

from openai import OpenAI

from .. import constants
from .categories import NF_CATEGORIES
from .llm_requests import PageId
from .pdf import split_pdf_pages
from .prompts import PromptSet, extraction_prompt_with_hint
from .responses import (
    EMPTY_USAGE,
    ClassificationResult,
    ExtractionResult,
    ModelOutput,
    parse_classification,
    parse_extraction,
)


@dataclass(frozen=True)
class DirectResult:
    """Respostas de um PDF processado por chamadas diretas."""

    total_pages: int
    classifications: list[ClassificationResult]
    extractions: list[ExtractionResult]


def call_direct(client: OpenAI, prompt: str, page_b64: str, page: PageId) -> ModelOutput:
    """Envia uma página pelo endpoint compatível com OpenAI do Bifrost.

    :param client: Cliente do Bifrost.
    :param prompt: Texto do prompt.
    :param page_b64: PDF de uma página em base64.
    :param page: Página enviada.
    :returns: Resposta crua; exceções viram ``error``.
    """
    config = constants.GENERATION_CONFIG
    try:
        response = client.chat.completions.create(
            model=f"{constants.BIFROST_PROVIDER}/{constants.MODEL_NAME}",
            messages=[
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": prompt},
                        {
                            "type": "file",
                            "file": {
                                "filename": f"{page.pdf_name}_p{page.page_number}.pdf",
                                "file_data": f"data:application/pdf;base64,{page_b64}",
                            },
                        },
                    ],
                }
            ],
            temperature=config["temperature"],
            top_p=config["topP"],
            max_tokens=config["maxOutputTokens"],
            response_format={"type": "json_object"},
        )
    except Exception as exc:
        return ModelOutput(page, None, dict(EMPTY_USAGE), f"Chamada direta falhou: {exc}")
    text = response.choices[0].message.content if response.choices else None
    usage = {
        "prompt_tokens": getattr(response.usage, "prompt_tokens", 0) or 0,
        "completion_tokens": getattr(response.usage, "completion_tokens", 0) or 0,
        "total_tokens": getattr(response.usage, "total_tokens", 0) or 0,
    }
    return ModelOutput(page, text, usage, None if text else "Resposta sem texto.")


def process_pdf_direct(
    client: OpenAI, pdf_name: str, pdf_bytes: bytes, prompts: PromptSet, max_workers: int = 8
) -> DirectResult:
    """Classifica todas as páginas e extrai as classificadas como documento fiscal.

    :param client: Cliente do Bifrost.
    :param pdf_name: Nome do PDF sem extensão.
    :param pdf_bytes: Conteúdo do PDF.
    :param prompts: Prompts a usar.
    :param max_workers: Chamadas simultâneas.
    :returns: Respostas no mesmo formato do batch.
    :raises ValueError: Se o PDF não puder ser lido.
    """
    pages = split_pdf_pages(pdf_bytes)

    def classify(number: int) -> ClassificationResult:
        """Classifica uma página do PDF."""
        page = PageId(pdf_name, number)
        return parse_classification(call_direct(client, prompts.classification_text, pages[number - 1], page))

    def extract(item: ClassificationResult) -> ExtractionResult:
        """Extrai os dados de uma página já classificada como documento fiscal."""
        prompt = extraction_prompt_with_hint(prompts.extraction_text, item.category)
        return parse_extraction(call_direct(client, prompt, pages[item.page.page_number - 1], item.page))

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        classifications = list(pool.map(classify, range(1, len(pages) + 1)))
        nf_pages = [item for item in classifications if item.category in NF_CATEGORIES]
        extractions = list(pool.map(extract, nf_pages))
    return DirectResult(len(pages), classifications, extractions)
