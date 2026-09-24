"""Integration-style test: confirms ``result_adapter.build_pdf_results_from_batch``
produces a shape that ``metadata.build_extracao_pagina_rows`` (the real
synchronous-path function, unmodified) can consume without errors.

This is the contract the whole batch path depends on — see
``utils/result_adapter.py``'s module docstring.
"""

from __future__ import annotations

from pipelines.rj_iplanrio__nf_agent.utils.batch import result_adapter
from pipelines.rj_iplanrio__nf_agent.utils.processing import metadata


def test_batch_results_feed_build_extracao_pagina_rows_end_to_end():
    classification_rows = [
        result_adapter.ClassificationOutputRow("doc.pdf", 1, "Nenhuma das Opções", "sem documento", {}, None),
        result_adapter.ClassificationOutputRow("doc.pdf", 2, "NFS-e", "tem CNPJ e valor", {}, None),
    ]
    extraction_rows = [
        result_adapter.ExtractionOutputRow(
            "doc.pdf",
            2,
            {
                "possui_nota_fiscal": True,
                "quantidade_notas_fiscais": 1,
                "notas_fiscais": [
                    {
                        "numero_nf": "123",
                        "pagina": 1,  # model's own local page number — must get remapped
                        "tipo_documento": "NFS-e",
                        "valor_total": 150.0,
                    }
                ],
            },
            {},
            None,
        )
    ]

    pdf_results = result_adapter.build_pdf_results_from_batch(
        classification_rows, extraction_rows, total_pages_by_pdf={"doc.pdf": 2}
    )

    pdf_tasks = [{"pdf_name": "doc.pdf"}]
    rows = metadata.build_extracao_pagina_rows(pdf_tasks=pdf_tasks, pdf_results=pdf_results)

    assert len(rows) == 2  # one row per page, exactly like the synchronous path

    page1, page2 = sorted(rows, key=lambda r: r["pagina"])
    assert page1["pipeline_status"] == "ok"
    assert page1["tipo_documento_classificacao"] == "Nenhuma das Opções"
    assert page1["tipo_documento_extracao"] is None

    assert page2["pipeline_status"] == "ok"
    assert page2["tipo_documento_classificacao"] == "NFS-e"
    assert page2["tipo_documento_extracao"] == "NFS-e"
    assert page2["numero_documento"] == "123"
    assert page2["valor_documento"] == 150.0


def test_pdf_with_processing_error_produces_erro_processamento_rows():
    classification_rows = [
        result_adapter.ClassificationOutputRow("bad.pdf", 1, None, "", {}, "Vertex batch row had no usable response"),
    ]

    pdf_results = result_adapter.build_pdf_results_from_batch(
        classification_rows, extraction_rows=[], total_pages_by_pdf={"bad.pdf": 1}
    )

    pdf_tasks = [{"pdf_name": "bad.pdf"}]
    rows = metadata.build_extracao_pagina_rows(pdf_tasks=pdf_tasks, pdf_results=pdf_results)

    assert len(rows) == 1
    assert rows[0]["pipeline_status"] == "erro_processamento"
