# -*- coding: utf-8 -*-
"""Motivo independente (categoria_pai nulo): vale pro macrotema inteiro, em qualquer tema."""

import pandas as pd

from pipelines.rj_crm__agentforce_classificacao_llm.tasks import taxonomia
from pipelines.rj_crm__agentforce_classificacao_llm.tasks.juiz import ConfigJuiz

CRIADO = pd.Timestamp("2026-11-20 10:00:00")


def _regra(agrupamento="Assistência", categoria_pai=None, nome="Sem benefício", palavra="beneficio"):
    return {
        "agrupamento": agrupamento, "categoria_pai": categoria_pai, "nome_funcao": f"f_{nome}", "nome": nome,
        "criado_em": CRIADO, "funcao": lambda texto: palavra in texto,
    }


def _sessao(**kw):
    base = {"conteudo_relevante": True, "macrotema_principal": "Assistência", "motivo": "perdi o beneficio"}
    return pd.Series({**base, **kw})


def test_candidato_sem_pai_casa_em_qualquer_tema_do_macrotema():
    regras = {"Assistência": [_regra()]}
    for temas in ({"Bolsa Família"}, {"CadÚnico"}, set()):
        r = taxonomia._candidatos(_sessao(), "motivo", "macrotema_principal", regras, temas_da_sessao=temas)
        assert [x["nome"] for x in r] == ["Sem benefício"]


def test_candidato_sem_pai_nao_casa_em_outro_macrotema():
    regras = {"Assistência": [_regra()]}
    r = taxonomia._candidatos(_sessao(macrotema_principal="Educação"), "motivo", "macrotema_principal", regras, {"X"})
    assert r == []


def test_candidato_com_pai_continua_exigindo_o_tema():
    regras = {"Assistência": [_regra(categoria_pai="Bolsa Família")]}
    assert taxonomia._candidatos(_sessao(), "motivo", "macrotema_principal", regras, {"CadÚnico"}) == []
    assert taxonomia._candidatos(_sessao(), "motivo", "macrotema_principal", regras, {"Bolsa Família"})


def test_par_para_cada_tema_da_sessao():
    d = [(_regra(), True, None)]
    assert taxonomia._pares_tema_motivo(d, ["A", "B"]) == [
        {"tema_nome": "A", "motivo_nome": "Sem benefício"}, {"tema_nome": "B", "motivo_nome": "Sem benefício"},
    ]


def test_par_com_tema_nulo_quando_sessao_sem_tema():
    assert taxonomia._pares_tema_motivo([(_regra(), None, None)], []) == [
        {"tema_nome": None, "motivo_nome": "Sem benefício"}
    ]


def test_par_ignora_recusado_e_mantem_modelo_antigo():
    d = [(_regra(), False, None), (_regra(categoria_pai="A", nome="Antigo"), True, None)]
    assert taxonomia._pares_tema_motivo(d, ["A", "B"]) == [{"tema_nome": "A", "motivo_nome": "Antigo"}]


def test_julgamento_reaproveitado_para_motivo_independente():
    cfg = ConfigJuiz.__new__(ConfigJuiz)
    object.__setattr__(cfg, "modelo", "m")
    object.__setattr__(cfg, "prompt_versao", "v1")
    regra = _regra()
    item = {"motivo_nome": regra["nome"], "nome_funcao": regra["nome_funcao"],
            "regra_criado_em": CRIADO.to_pydatetime(), "pertence": True, "por_que": "ok"}
    row = {"modelo_juiz": "m", "prompt_versao_juiz": "v1", "motivo_julgamento": [item]}
    ja = taxonomia._julgamentos_reaproveitaveis(row, cfg, "motivo_julgamento", "motivo")
    chave = ("motivo", regra["nome_funcao"], taxonomia._versao(regra["criado_em"]))
    assert ja[chave] == (True, "ok")
