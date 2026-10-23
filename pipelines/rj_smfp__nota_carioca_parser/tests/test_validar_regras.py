"""Garantias do conjunto de regras: toda regra registrada tem teste e nenhuma ignora um erro sem relação."""

import pytest

from pipelines.rj_smfp__nota_carioca_parser.tests import (
    test_validar_regras_estrutura as estrutura,
)
from pipelines.rj_smfp__nota_carioca_parser.tests import test_validar_regras_valores as valores
from pipelines.rj_smfp__nota_carioca_parser.tests import test_validar_regras_versoes as versoes
from pipelines.rj_smfp__nota_carioca_parser.tests.auxiliares import faceta, inesperado
from pipelines.rj_smfp__nota_carioca_parser.utils.tolerancias import REGRAS_COMUNS, REGRAS_V100, REGRAS_V101
from pipelines.rj_smfp__nota_carioca_parser.utils.versao import Versao
from pipelines.rj_smfp__nota_carioca_parser.utils.tolerancias import regras_para

TODAS = list(dict.fromkeys([*REGRAS_COMUNS, *REGRAS_V100, *REGRAS_V101]))


def test_validar_toda_regra_registrada_tem_teste_que_a_exercita():
    cobertas = valores.COBERTAS | estrutura.COBERTAS | versoes.COBERTAS
    assert {r.__name__ for r in TODAS} - {r.__name__ for r in cobertas} == set()


def test_validar_nomes_das_regras_sao_unicos():
    assert len({r.__name__ for r in TODAS}) == len(TODAS)


@pytest.mark.parametrize("regra", TODAS, ids=[r.__name__ for r in TODAS])
def test_validar_regra_nao_ignora_erros_sem_relacao(regra):
    assert not regra(faceta("tpAmb", "enumeration", "9", "infDPS"))
    assert not regra(inesperado("zzz", "yyy", "www"))


def test_validar_cada_versao_recebe_as_regras_comuns_mais_as_da_propria_versao():
    r100, r101 = regras_para(Versao(1, 0)), regras_para(Versao(1, 1))
    assert r100 == (*REGRAS_COMUNS, *REGRAS_V100)
    assert r101 == (*REGRAS_COMUNS, *REGRAS_V101)
    assert valores_cstat(r100) == {"tolerar_cstat_107"}
    assert valores_cstat(r101) == {"tolerar_cstat_101"}


def valores_cstat(regras) -> set[str]:
    return {r.__name__ for r in regras if r.__name__.startswith("tolerar_cstat_")}
