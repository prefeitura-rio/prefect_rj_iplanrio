"""Reforço de logging só desta pipeline — não mexe em ``src/prefect_rj_iplanrio/`` (código
compartilhado por todo o monorepo; outras pipelines usam ``get_logger`` sem esse problema).

Algumas das nossas dependências pesadas (LightGBM, Optuna, SHAP, numba — importadas
transitivamente por ``tasks/retreino/treinar.py`` e ``relatorio.py``, e que nenhuma outra
pipeline deste repo carrega) mexem na configuração do logger raiz do Python na hora da
importação, e isso pode abafar o nível ``INFO`` que ``prefect_rj_iplanrio.logging.get_logger``
espera que já esteja ligado — os logs desta pipeline (e só desta) somem, mesmo chamando
``logger.info(...)`` do jeito certo.
"""

import logging

NOME_RAIZ_PIPELINE = "pipelines.rj_crm__modelo_qualidade_telefone"


def garante_logging_visivel() -> None:
    """Garante nível ``INFO`` e um handler pro namespace desta pipeline.

    Chamar 1x, na primeira linha do ``@flow`` — os imports pesados (topo de ``flow.py``)
    já rodaram nesse ponto, então esta chamada é a última palavra sobre a configuração,
    não importa o que LightGBM/Optuna/SHAP tenham feito no logger raiz ao serem importados.
    Idempotente (não duplica handler se chamada mais de uma vez).
    """
    logger_raiz = logging.getLogger(NOME_RAIZ_PIPELINE)
    logger_raiz.setLevel(logging.INFO)
    if not logger_raiz.handlers:
        handler = logging.StreamHandler()  # stdout — o job do Prefect/Kubernetes já captura isso
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
        logger_raiz.addHandler(handler)
