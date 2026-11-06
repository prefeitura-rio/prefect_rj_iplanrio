"""Convenção de ambiente: ``staging`` e ``prod``, cada um com o seu dataset, como o extrator do ADN grava."""

import inspect

from pipelines.rj_smfp__nota_carioca_parser import constants
from pipelines.rj_smfp__nota_carioca_parser import flow as modulo_flow


def test_valores_de_ambiente_sao_staging_e_prod() -> None:
    assert (constants.AMBIENTE_PROD, constants.AMBIENTE_STAGING) == ("prod", "staging")


def test_flow_sem_parametros_le_prod_do_dataset_de_prod() -> None:
    parametros = inspect.signature(modulo_flow.rj_smfp__nota_carioca_parser.fn).parameters
    assert parametros["ambiente"].default == "prod"
    assert parametros["dataset_id"].default == "raw_nfse_nacional"
