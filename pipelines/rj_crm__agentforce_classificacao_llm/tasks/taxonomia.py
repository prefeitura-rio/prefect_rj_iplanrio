# -*- coding: utf-8 -*-
"""
Aplicação das regras de tema/motivo (funções Python já induzidas e validadas no repo
`clustering`, tabela `taxonomia_regras`) sobre o `resumo` das sessões recém-classificadas.

As funções já existem (induzidas e validadas por precisão no clustering_v3), aqui só são
executadas e propõem candidatos. Cada candidato passa pelo juiz (tasks/juiz.py), que só
tira rótulo, nunca põe: tema_nome/tema_motivo = candidatos aceitos + os sem decisão do juiz
(pertence None, o rótulo da regra fica). Todos os candidatos, com a decisão e o por_que,
ficam em tema_julgamento/tema_motivo_julgamento. Ordem: regra de tema -> juiz -> regra de
motivo (só nos temas que ficaram) -> juiz — ver aplica_taxonomia.

Catálogo (`taxonomia_regras`) é dado, não código: promovido manualmente pra lá depois da
validação de precisão (não existe promoção automática — função gerada por LLM não vira
produção sem humano no meio). Mesma tabela serve tema e motivo (coluna `etapa`) — motivo
tem `categoria_pai` preenchido (o tema ao qual pertence) e só é avaliado em sessão que já
tenha aquele tema em `tema_nome` (ver filtro por categoria_pai em `_candidatos`); tema
tem `categoria_pai` nulo (é top-level, não pertence a nada). Motivo independente (clustering_v3)
também tem `categoria_pai` nulo: vale pro macrotema inteiro, em qualquer tema, e é gravado em
`motivo_nome`/`motivo_julgamento`, dimensão paralela ao tema. `tema_motivo` (pares) é a ponte da
migração (ver `_pares_tema_motivo`) e sai depois.

`RulesSandbox` portado de clustering/modules/rules_sandbox.py — mesma lógica, sem mudança
de comportamento: valida o código via AST antes de compilar (bloqueia import fora de
re/unicodedata, eval/exec/open/os/subprocess, acesso a atributo dunder) e rejeita função
que dependa de algo fora dela mesma (garante que regra_python é autocontida).
"""

from __future__ import annotations

import ast
import re
import unicodedata
from datetime import date, datetime, timezone
from typing import ClassVar

import pandas as pd
from google.api_core.exceptions import NotFound
from google.cloud import bigquery
from prefect import task
from prefect.cache_policies import NO_CACHE
from prefect.deployments import run_deployment

from pipelines.rj_crm__agentforce_classificacao_llm.constants import QUERIES_ANCHOR
from pipelines.rj_crm__agentforce_classificacao_llm.tasks.juiz import ConfigJuiz, julga
from pipelines.rj_crm__agentforce_classificacao_llm.tasks.load import SCHEMA, _full_table_id
from pipelines.rj_crm__agentforce_classificacao_llm.utils.bigquery import get_bq_client
from prefect_rj_iplanrio.sql import load_query

# Coluna da sessão que dá o valor de cada agrupamento_tipo do catálogo. Hierarquia:
# agrupamento (hoje o macrotema) > tema > motivo — a sessão só recebe tema do seu próprio
# agrupamento, e motivo só do seu agrupamento E de um tema que ela já recebeu.
COLUNA_SESSAO_POR_AGRUPAMENTO = {"macrotema": "macrotema_principal"}


def _normaliza_resumo(texto: str) -> str:
    """Baixa a caixa, remove acentos e colapsa espaços — mesma transformação de
    clustering/modules/text_utils.py::normaliza. As regras em `taxonomia_regras` são
    induzidas e validadas ali chamando a função sobre `normaliza(t)` (ver
    modules/taxonomy.py::_imprime_categoria_e_regra), não sobre o texto cru — termos
    como 'transferencia' (sem acento) só batem se o texto já estiver normalizado.
    Sem este passo aqui, `regra["funcao"](resumo)` recebia o `resumo` cru (com acento,
    caixa mista) e a taxa de match caía bem abaixo da medida na indução (ex.: regra
    'Transferência escolar' medida em 18,2% da base na indução, ~1,5% em produção)."""
    texto = str(texto).strip().lower()
    texto = unicodedata.normalize("NFKD", texto).encode("ascii", "ignore").decode("ascii")
    return re.sub(r"\s+", " ", texto)


# ---------------------------------------------------------------------------
# Sandbox — portado de clustering/modules/rules_sandbox.py, sem alteração de lógica
# ---------------------------------------------------------------------------


class _RemoveImports(ast.NodeTransformer):
    def visit_Import(self, _node):
        return None

    def visit_ImportFrom(self, _node):
        return None


class RulesSandbox:
    NOMES_PROIBIDOS: ClassVar[set[str]] = {
        "eval", "exec", "open", "__import__", "compile", "globals", "locals",
        "vars", "getattr", "setattr", "delattr", "input", "os", "sys", "subprocess",
    }
    IMPORTS_PERMITIDOS: ClassVar[set[str]] = {"re", "unicodedata"}

    BUILTINS_SEGUROS: ClassVar[dict] = {
        "len": len, "any": any, "all": all, "str": str, "bool": bool, "int": int, "float": float,
        "min": min, "max": max, "sum": sum, "sorted": sorted, "list": list, "set": set, "tuple": tuple,
        "dict": dict, "range": range, "enumerate": enumerate, "zip": zip, "isinstance": isinstance,
        "True": True, "False": False, "None": None,
    }

    def is_safe(self, codigo):
        try:
            arvore = ast.parse(codigo)
        except SyntaxError as e:
            return False, f"código com erro de sintaxe: {e}"

        if not any(isinstance(n, ast.FunctionDef) for n in arvore.body):
            return False, "nenhuma função encontrada no bloco"

        for node in arvore.body:
            if isinstance(node, ast.FunctionDef):
                continue
            eh_import_ok = (
                isinstance(node, ast.Import) and all(a.name in self.IMPORTS_PERMITIDOS for a in node.names)
            ) or (isinstance(node, ast.ImportFrom) and node.module in self.IMPORTS_PERMITIDOS)
            if not eh_import_ok:
                return False, "só é permitido, no nível principal, funções e import de re/unicodedata"

        for node in ast.walk(arvore):
            if isinstance(node, ast.Import) and not all(a.name in self.IMPORTS_PERMITIDOS for a in node.names):
                return False, f"import não permitido: {[a.name for a in node.names]}"
            if isinstance(node, ast.ImportFrom) and node.module not in self.IMPORTS_PERMITIDOS:
                return False, f"import não permitido: {node.module}"
            if isinstance(node, ast.Attribute) and node.attr.startswith("__"):
                return False, "acesso a atributo dunder não permitido"
            if isinstance(node, ast.Name) and node.id in self.NOMES_PROIBIDOS:
                return False, f"uso de '{node.id}' não permitido"

        return True, None

    @staticmethod
    def _nomes_vinculados_localmente(no_func):
        nomes = {a.arg for a in no_func.args.args}
        nomes |= {a.arg for a in no_func.args.posonlyargs}
        nomes |= {a.arg for a in no_func.args.kwonlyargs}
        if no_func.args.vararg:
            nomes.add(no_func.args.vararg.arg)
        if no_func.args.kwarg:
            nomes.add(no_func.args.kwarg.arg)

        for node in ast.walk(no_func):
            if node is no_func:
                continue
            if isinstance(node, ast.Name) and isinstance(node.ctx, ast.Store):
                nomes.add(node.id)
            elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                nomes.add(node.name)
                nomes |= {a.arg for a in node.args.args}
            elif isinstance(node, ast.Lambda):
                nomes |= {a.arg for a in node.args.args}
        return nomes

    def referencias_externas(self, no_func):
        permitidos = set(self.BUILTINS_SEGUROS) | {"re", "unicodedata", no_func.name}
        permitidos |= self._nomes_vinculados_localmente(no_func)

        externas = set()
        for node in ast.walk(no_func):
            if isinstance(node, ast.Name) and isinstance(node.ctx, ast.Load) and node.id not in permitidos:
                externas.add(node.id)
        return externas

    def compile(self, codigo):
        seguro, motivo = self.is_safe(codigo)
        if not seguro:
            raise ValueError(f"bloco rejeitado por segurança: {motivo}")

        arvore = ast.parse(codigo)
        arvore_limpa = _RemoveImports().visit(arvore)
        ast.fix_missing_locations(arvore_limpa)
        funcoes_def = [n for n in arvore_limpa.body if isinstance(n, ast.FunctionDef)]

        resultado = {}
        for no_func in funcoes_def:
            nome = no_func.name
            externas = self.referencias_externas(no_func)
            if externas:
                print(
                    f"[TAXONOMIA] função '{nome}' rejeitada: referencia {sorted(externas)}, "
                    "não é parâmetro/variável dela mesma nem builtin/re/unicodedata"
                )
                continue

            globals_da_funcao = {"__builtins__": self.BUILTINS_SEGUROS, "re": re, "unicodedata": unicodedata}
            modulo_da_funcao = ast.Module(body=[no_func], type_ignores=[])
            # __builtins__ restrito a BUILTINS_SEGUROS acima (sem eval/exec/open/import/os/
            # subprocess) é o que torna esse exec seguro — é a sandbox em si, não um exec solto
            exec(ast.unparse(modulo_da_funcao), globals_da_funcao)
            resultado[nome] = {"funcao": globals_da_funcao[nome], "regra_python": ast.unparse(no_func)}

        return resultado


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------

_COLUNAS_CATALOGO = [
    "agrupamento_tipo", "agrupamento", "categoria_pai", "nome_funcao", "nome", "descricao", "inclui",
    "nao_inclui", "regra_python", "criado_em",
]


@task(log_prints=True, retries=2, retry_delay_seconds=30)
def carrega_catalogo_regras(
    project_id: str, dataset_id: str, table_id: str, etapa: str, agrupamento_tipo: str
) -> pd.DataFrame:
    """Lê as regras ativas de uma etapa ('tema' ou 'motivo') de um agrupamento_tipo só
    (hoje 'macrotema') — queries/carrega_catalogo_regras.sql. Regra de outro tipo, se
    existir no catálogo, fica de fora — a sessão é classificada num nível de agregação por
    vez. Tabela é gerenciada por fora desta pipeline (promovida do clustering_v3, ver
    docstring do módulo) — se ainda não existir, trata como "sem regras hoje" em vez de
    falhar o flow inteiro."""
    if agrupamento_tipo not in COLUNA_SESSAO_POR_AGRUPAMENTO:
        raise ValueError(
            f"agrupamento_tipo '{agrupamento_tipo}' sem coluna de sessão em COLUNA_SESSAO_POR_AGRUPAMENTO."
        )
    client = get_bq_client(project_id)
    full_id = f"{project_id}.{dataset_id}.{table_id}"
    query = load_query(QUERIES_ANCHOR, "carrega_catalogo_regras", full_id=full_id)
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("etapa", "STRING", etapa),
            bigquery.ScalarQueryParameter("agrupamento_tipo", "STRING", agrupamento_tipo),
        ]
    )
    try:
        df = client.query(query, job_config=job_config).to_dataframe()
    except NotFound:
        print(f"[TAXONOMIA] '{table_id}' não existe ainda — nenhuma regra de '{etapa}' aplicada nesta execução.")
        df = pd.DataFrame(columns=_COLUNAS_CATALOGO)

    n_sem_definicao = int((df["inclui"].isna() | df["nao_inclui"].isna()).sum()) if len(df) else 0
    print(
        f"[TAXONOMIA] {len(df)} regra(s) ativa(s) de '{etapa}' ({agrupamento_tipo}) carregadas do catálogo"
        f" ({n_sem_definicao} sem inclui/nao_inclui — não passam pelo juiz)."
    )
    return df


def _vazio(valor) -> bool:
    """None, NaN do BigQuery (float('nan') é truthy) ou string em branco."""
    if valor is None:
        return True
    if isinstance(valor, float) and pd.isna(valor):
        return True
    return not str(valor).strip()


def _lista(valor) -> list:
    """Coluna REPEATED: lista Python, array numpy (to_dataframe do BigQuery) ou None/NaN.
    Array numpy não aceita `valor or []` (truthiness ambígua) — por isso a conversão."""
    if valor is None or (isinstance(valor, float) and pd.isna(valor)):
        return []
    return list(valor)


def _versao(criado_em) -> int | None:
    """criado_em da regra como inteiro (ns) — chave de comparação estável entre o Timestamp
    do catálogo e o datetime que volta do BigQuery no recálculo."""
    if criado_em is None or pd.isna(criado_em):
        return None
    return pd.Timestamp(criado_em).value


def _datahora(criado_em):
    return None if criado_em is None or pd.isna(criado_em) else pd.Timestamp(criado_em).to_pydatetime()


def _compila_catalogo(df_regras: pd.DataFrame, rotulo_etapa: str) -> tuple[str | None, dict[str, list[dict]]]:
    """Compila (sandbox) as regras do catálogo, agrupadas pelo agrupamento (macrotema).
    Devolve (coluna da sessão que dá o agrupamento, {agrupamento: [regra, ...]}). Regra
    rejeitada pelo sandbox é pulada e logada."""
    if df_regras.empty:
        return None, {}
    tipos = set(df_regras["agrupamento_tipo"])
    if len(tipos) != 1:
        raise ValueError(f"catálogo de '{rotulo_etapa}' mistura agrupamento_tipo {sorted(tipos)} — esperado um só.")
    coluna_agrupamento = COLUNA_SESSAO_POR_AGRUPAMENTO[tipos.pop()]

    sandbox = RulesSandbox()
    regras_por_agrupamento: dict[str, list[dict]] = {}
    n_rejeitadas = 0
    for row in df_regras.itertuples():
        try:
            compiladas = sandbox.compile(row.regra_python)
        except ValueError as e:
            n_rejeitadas += 1
            print(f"[TAXONOMIA] regra '{row.nome_funcao}' (agrupamento={row.agrupamento}) rejeitada pelo sandbox: {e}")
            continue
        for info in compiladas.values():
            regras_por_agrupamento.setdefault(row.agrupamento, []).append({
                "agrupamento": row.agrupamento,
                "categoria_pai": None if _vazio(row.categoria_pai) else row.categoria_pai,
                "nome_funcao": row.nome_funcao,
                "nome": row.nome,
                "descricao": row.descricao,
                "inclui": None if _vazio(row.inclui) else row.inclui,
                "nao_inclui": None if _vazio(row.nao_inclui) else row.nao_inclui,
                "criado_em": row.criado_em,
                "funcao": info["funcao"],
            })
    n_ok = sum(len(v) for v in regras_por_agrupamento.values())
    print(f"[TAXONOMIA] '{rotulo_etapa}': {n_ok} regra(s) compiladas, {n_rejeitadas} rejeitada(s) pelo sandbox.")
    return coluna_agrupamento, regras_por_agrupamento


def _candidatos(row, coluna_texto: str, coluna_agrupamento: str | None, regras_por_agrupamento: dict,
                temas_da_sessao: set | None = None) -> list[dict]:
    """Regras do catálogo que batem no texto normalizado (ver `_normaliza_resumo`) da
    `coluna_texto` da sessão: `resumo` pra tema, `motivo` pra motivo — o texto sobre o qual
    a regra foi induzida e teve a precisão medida (aplicar a regra de motivo sobre o
    `resumo` muda o que ela pega e invalida a precisão).

    Hierárquico: agrupamento (hoje o macrotema) > tema > motivo.
    - A sessão só é avaliada contra as regras do SEU agrupamento (macrotema_principal, ver
      COLUNA_SESSAO_POR_AGRUPAMENTO). Tema de outro macrotema nunca é herdado.
    - Regra com `categoria_pai` (etapa='motivo') só é avaliada se esse tema está em
      `temas_da_sessao` — os temas que FICARAM depois do juiz (ver aplica_taxonomia). Um
      tema homônimo em outro macrotema não empresta motivo.

    Sessão não relevante, sem texto, sem agrupamento ou de agrupamento sem regra -> [].
    Função que falha numa sessão é pulada e logada — nunca derruba a sessão."""
    if coluna_agrupamento is None:
        return []
    # nullable boolean (pd.NA) não aceita `not x` direto — comparação explícita
    if row.get("conteudo_relevante") is not True:
        return []
    texto = row.get(coluna_texto)
    if _vazio(texto):
        return []
    agrupamento = row.get(coluna_agrupamento)
    if agrupamento is None or pd.isna(agrupamento):
        return []
    regras = regras_por_agrupamento.get(agrupamento, [])
    if not regras:
        return []

    temas_da_sessao = temas_da_sessao or set()
    texto_norm = _normaliza_resumo(texto)
    matches = []
    for regra in regras:
        if regra["categoria_pai"] is not None and regra["categoria_pai"] not in temas_da_sessao:
            continue  # motivo de um tema que esta sessão não tem — fora de escopo
        try:
            if regra["funcao"](texto_norm):
                matches.append(regra)
        except Exception as e:  # função individual não pode derrubar a sessão inteira
            print(
                f"[TAXONOMIA] falha ao avaliar '{regra['nome']}' na sessão {row.get('id_sessao')}: "
                f"{type(e).__name__}: {e}"
            )
    return matches


def _julgamentos_reaproveitaveis(row, cfg_juiz: ConfigJuiz, coluna: str, etapa: str) -> dict:
    """{chave: (pertence, por_que)} dos julgamentos já gravados na sessão, só se foram
    feitos pelo mesmo modelo e versão de prompt do juiz atual. Chave = (etapa, nome_funcao,
    versão da regra): mudou a regra, julga de novo. Itens com
    pertence None também são reaproveitados (sem decisão do juiz não se julga de novo)."""
    if row.get("modelo_juiz") != cfg_juiz.modelo or row.get("prompt_versao_juiz") != cfg_juiz.prompt_versao:
        return {}
    saida = {}
    for item in _lista(row.get(coluna)):
        chave = (etapa, item.get("nome_funcao"), _versao(item.get("regra_criado_em")))
        pertence = item.get("pertence")
        saida[chave] = (None if pertence is None or pd.isna(pertence) else bool(pertence), item.get("por_que"))
    return saida


def _decide(  # noqa: PLR0913 — tudo nomeado (keyword-only)
    df: pd.DataFrame,
    *,
    candidatos: pd.Series,
    etapa: str,
    coluna_texto: str,
    escopo_de,
    cfg_juiz: ConfigJuiz,
    reaproveita: dict | None,
) -> pd.Series:
    """Decisão do juiz pra cada candidato de cada sessão: Series (mesmo índice de df) de
    listas [(regra, pertence, por_que)] na ordem dos candidatos. Reaproveita o que estiver
    em `reaproveita` ({índice da sessão: {chave: (pertence, por_que)}}); o resto vai pro
    juiz numa chamada só (tasks/juiz.py agrupa por categoria em lotes)."""
    decisoes = {i: [None] * len(c) for i, c in candidatos.items()}
    pedidos, destinos = [], []
    n_reaproveitados = 0
    for i, cands in candidatos.items():
        ja_julgados = (reaproveita or {}).get(i, {})
        for k, regra in enumerate(cands):
            chave = (etapa, regra["nome_funcao"], _versao(regra["criado_em"]))
            if chave in ja_julgados:
                decisoes[i][k] = (regra, *ja_julgados[chave])
                n_reaproveitados += 1
                continue
            pedidos.append({"escopo": escopo_de(regra), "regra": regra, "texto": df.at[i, coluna_texto]})
            destinos.append((i, k))

    for (i, k), (pertence, por_que) in zip(destinos, julga(cfg_juiz, pedidos), strict=True):
        decisoes[i][k] = (candidatos[i][k], pertence, por_que)

    todos = [d for lista in decisoes.values() for d in lista]
    print(
        f"[TAXONOMIA] '{etapa}': {len(todos)} candidato(s) — {n_reaproveitados} reaproveitado(s), "
        f"{len(pedidos)} julgado(s) agora; aceitos {sum(d[1] is True for d in todos)}, "
        f"recusados {sum(d[1] is False for d in todos)}, sem decisão {sum(d[1] is None for d in todos)}."
    )
    return pd.Series(decisoes, dtype=object).reindex(df.index)


def _pares_tema_motivo(decisoes, temas_da_sessao: list) -> list[dict]:
    """Pares {tema_nome, motivo_nome} dos motivos aceitos. Motivo com tema pai (modelo antigo):
    um par com o pai. Motivo independente (categoria_pai nulo): um par por tema da sessão, ou
    um par com tema_nome NULL se a sessão não tem tema."""
    pares = []
    for r, p, _ in decisoes:
        if p is False:
            continue
        if r["categoria_pai"] is not None:
            pares.append({"tema_nome": r["categoria_pai"], "motivo_nome": r["nome"]})
        else:
            for tema in temas_da_sessao or [None]:
                pares.append({"tema_nome": tema, "motivo_nome": r["nome"]})
    return pares


@task(log_prints=True, cache_policy=NO_CACHE)
def aplica_taxonomia(
    df_final: pd.DataFrame,
    df_regras_tema: pd.DataFrame,
    df_regras_motivo: pd.DataFrame,
    cfg_juiz: ConfigJuiz,
    reaproveita_julgamentos: bool = False,
) -> pd.DataFrame:
    """Tema e motivo da sessão: regra de tema -> juiz -> regra de motivo -> juiz.

    - Regra (regex) propõe candidatos; o juiz (tasks/juiz.py) confirma cada um sobre o
      mesmo texto da regra. Só tira rótulo, nunca põe.
    - tema_nome / tema_motivo = candidatos com pertence True ou None (juiz sem resposta:
      fica o rótulo da regra). Motivo só é avaliado nos temas que FICARAM — tema recusado
      não leva motivo.
    - tema_julgamento / tema_motivo_julgamento = todos os candidatos, com pertence e
      por_que; modelo_juiz / prompt_versao_juiz preenchidos só em sessão com candidato.
    - reaproveita_julgamentos (recálculo): df_final traz os julgamentos já gravados;
      candidato com mesma regra (nome_funcao + criado_em), mesmo modelo e mesmo prompt do
      juiz não é julgado de novo.

    cache_policy=NO_CACHE: cfg_juiz carrega a BF_KEY e não deve virar chave de cache."""
    if df_final.empty:
        return df_final
    df = df_final.copy()

    col_agr_tema, regras_tema = _compila_catalogo(df_regras_tema, "tema")
    col_agr_motivo, regras_motivo = _compila_catalogo(df_regras_motivo, "motivo")
    # escopo do prompt igual ao da 08_precisao: motivo cita o nome_funcao do tema pai
    tema_funcao = {
        (r["agrupamento"], r["nome"]): r["nome_funcao"] for regras in regras_tema.values() for r in regras
    }

    def _escopo_tema(regra):
        return f"macrotema '{regra['agrupamento']}'"

    def _escopo_motivo(regra):
        if regra["categoria_pai"] is None:  # motivo independente (mesmo texto do clustering_v3, escopo_texto)
            return f"motivos do macrotema '{regra['agrupamento']}' (o porquê do cidadão, em qualquer tema)"
        tema = tema_funcao.get((regra["agrupamento"], regra["categoria_pai"]), regra["categoria_pai"])
        return f"tema '{tema}' do macrotema '{regra['agrupamento']}'"

    reaproveita_tema = reaproveita_motivo = None
    if reaproveita_julgamentos:
        reaproveita_tema = {
            i: _julgamentos_reaproveitaveis(row, cfg_juiz, "tema_julgamento", "tema") for i, row in df.iterrows()
        }
        reaproveita_motivo = {
            i: _julgamentos_reaproveitaveis(row, cfg_juiz, "motivo_julgamento", "motivo")
            for i, row in df.iterrows()
        }

    # 1. tema
    cand_tema = df.apply(lambda row: _candidatos(row, "resumo", col_agr_tema, regras_tema), axis=1)
    dec_tema = _decide(
        df, candidatos=cand_tema, etapa="tema", coluna_texto="resumo", escopo_de=_escopo_tema,
        cfg_juiz=cfg_juiz, reaproveita=reaproveita_tema,
    )
    df["tema_julgamento"] = [
        [
            {"tema_nome": r["nome"], "nome_funcao": r["nome_funcao"], "regra_criado_em": _datahora(r["criado_em"]),
             "pertence": p, "por_que": q}
            for r, p, q in decisoes
        ]
        for decisoes in dec_tema
    ]
    df["tema_nome"] = [[r["nome"] for r, p, _ in decisoes if p is not False] for decisoes in dec_tema]

    # 2. motivo, só nos temas que ficaram
    cand_motivo = pd.Series(
        [
            _candidatos(row, "motivo", col_agr_motivo, regras_motivo, temas_da_sessao=set(temas))
            for (_, row), temas in zip(df.iterrows(), df["tema_nome"], strict=True)
        ],
        index=df.index,
        dtype=object,
    )
    dec_motivo = _decide(
        df, candidatos=cand_motivo, etapa="motivo", coluna_texto="motivo", escopo_de=_escopo_motivo,
        cfg_juiz=cfg_juiz, reaproveita=reaproveita_motivo,
    )
    df["motivo_julgamento"] = [
        [
            {"motivo_nome": r["nome"], "nome_funcao": r["nome_funcao"],
             "regra_criado_em": _datahora(r["criado_em"]), "pertence": p, "por_que": q}
            for r, p, q in decisoes
        ]
        for decisoes in dec_motivo
    ]
    df["motivo_nome"] = [[r["nome"] for r, p, _ in decisoes if p is not False] for decisoes in dec_motivo]
    # ponte (sai com a migração): pares tema/motivo no formato antigo
    df["tema_motivo_julgamento"] = [
        [
            {"tema_nome": r["categoria_pai"], "motivo_nome": r["nome"], "nome_funcao": r["nome_funcao"],
             "regra_criado_em": _datahora(r["criado_em"]), "pertence": p, "por_que": q}
            for r, p, q in decisoes
        ]
        for decisoes in dec_motivo
    ]
    df["tema_motivo"] = [
        _pares_tema_motivo(decisoes, temas)
        for decisoes, temas in zip(dec_motivo, df["tema_nome"], strict=True)
    ]

    tem_candidato = [bool(a) or bool(b) for a, b in zip(dec_tema, dec_motivo, strict=True)]
    df["modelo_juiz"] = [cfg_juiz.modelo if t else None for t in tem_candidato]
    df["prompt_versao_juiz"] = [cfg_juiz.prompt_versao if t else None for t in tem_candidato]

    print(
        f"[TAXONOMIA] {(df['tema_nome'].str.len() > 0).sum()} sessão(ões) com tema, "
        f"{(df['tema_motivo'].str.len() > 0).sum()} com motivo (de {len(df)})."
    )
    return df


# ---------------------------------------------------------------------------
# Recálculo de taxonomia (parâmetro recalcula_taxonomia do flow) — ver flow.py.
#
# Cenário: a taxonomia (catálogo de regras) muda — uma regra é corrigida, uma
# categoria é adicionada — e as sessões já classificadas precisam refletir isso,
# sem rechamar a classificação principal e sem sobrescrever relacao_hsm/resumo/motivo/etc.
# (só as colunas de tema/motivo e do juiz mudam). As regras rodam de novo sobre o
# resumo/motivo salvos; o juiz só é chamado pros candidatos sem julgamento reaproveitável
# (regra nova ou alterada, ou juiz de outro modelo/versão). MERGE parcial em todas as
# sessões relevantes da tabela de classificação e, em seguida, full refresh do ramo
# Salesforce da chatbot no dbt — os incrementais dele não reprocessam o passado
# sozinhos quando a tabela de classificação muda.
# ---------------------------------------------------------------------------

_COLUNAS_TAXONOMIA = [
    "tema_nome", "tema_motivo", "tema_julgamento", "tema_motivo_julgamento", "motivo_nome", "motivo_julgamento",
    "modelo_juiz", "prompt_versao_juiz",
]


@task(log_prints=True, retries=2, retry_delay_seconds=30)
def extrai_sessoes_para_recalculo_taxonomia(project_id: str, dataset_id: str, table_id: str) -> pd.DataFrame:
    """Lê da própria tabela destino (queries/extrai_sessoes_recalculo_taxonomia.sql) só o
    que a taxonomia precisa: id_sessao, resumo (texto das regras de tema), motivo (texto
    das regras de motivo), macrotema_principal, conteudo_relevante e os julgamentos atuais
    (reaproveitados quando regra, modelo e prompt do juiz não mudaram). Todo o histórico,
    sem filtro de data."""
    client = get_bq_client(project_id)
    full_id = _full_table_id(project_id, dataset_id, table_id)
    query = load_query(QUERIES_ANCHOR, "extrai_sessoes_recalculo_taxonomia", full_id=full_id)
    df = client.query(query).to_dataframe()
    print(f"[TAXONOMIA] {len(df)} sessão(ões) relevante(s) carregada(s) da tabela destino pra recálculo de tema/motivo.")
    return df


@task(log_prints=True, retries=3, retry_delay_seconds=[30, 60, 120])
def atualiza_tema_motivo(
    df_tema: pd.DataFrame,
    project_id: str,
    dataset_id: str,
    table_id: str,
    tmp_table_id: str,
) -> int:
    """MERGE PARCIAL (queries/merge_tema_motivo.sql) — atualiza só as colunas de
    tema/motivo e do juiz (por id_sessao) na tabela destino, sem tocar em nenhuma outra
    coluna (relacao_hsm, resumo, motivo etc. ficam intactos). Diferente de
    carrega_classificacoes (tasks/load.py), que sobrescreve a linha inteira.

    Reaproveita a mesma tabela tmp da carga normal (schema físico já tem as REQUIRED
    id_sessao/classificado_em/data_particao) — como o MERGE só faz UPDATE SET nas colunas
    de taxonomia, os valores de classificado_em/data_particao carregados aqui são só
    placeholder pra satisfazer o schema da tmp: nunca chegam na tabela destino.

    Trunca a tmp ANTES de carregar também (não só depois do MERGE): se o MERGE falha, a
    task é repetida (retries) e, sem isso, cada tentativa empilhava outra cópia das
    linhas na tmp — MERGE com id_sessao repetido na origem falha de novo, sempre. O
    deployment de produção tem concurrency_limit 1, então nenhuma outra execução usa a
    tmp ao mesmo tempo."""
    if df_tema.empty:
        print("[TAXONOMIA] Nada a atualizar — nenhuma sessão com tema/motivo recalculado.")
        return 0

    client = get_bq_client(project_id)
    tmp_full = _full_table_id(project_id, dataset_id, tmp_table_id)
    destino_full = _full_table_id(project_id, dataset_id, table_id)

    agora = datetime.now(tz=timezone.utc)
    df_carga = df_tema[["id_sessao", *_COLUNAS_TAXONOMIA]].copy()
    df_carga["classificado_em"] = agora  # placeholder, nunca lido pelo MERGE (ver docstring)
    df_carga["data_particao"] = date.today()  # placeholder, idem

    colunas_tmp = {"id_sessao", "classificado_em", "data_particao", *_COLUNAS_TAXONOMIA}
    schema_tmp = [f for f in SCHEMA if f.name in colunas_tmp]

    client.query(load_query(QUERIES_ANCHOR, "trunca_tmp", tmp_full=tmp_full)).result()
    job_config = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_APPEND, schema=schema_tmp)
    job = client.load_table_from_dataframe(df_carga, tmp_full, job_config=job_config)
    job.result()
    if job.errors:
        raise RuntimeError(f"[TAXONOMIA] Erro ao carregar tmp: {job.errors}")

    merge_sql = load_query(QUERIES_ANCHOR, "merge_tema_motivo", destino_full=destino_full, tmp_full=tmp_full)
    merge_job = client.query(merge_sql)
    merge_job.result()
    linhas = merge_job.num_dml_affected_rows or 0

    client.query(load_query(QUERIES_ANCHOR, "trunca_tmp", tmp_full=tmp_full)).result()
    print(f"[TAXONOMIA] MERGE parcial concluído: {linhas} linha(s) de tema/motivo atualizada(s) em '{table_id}'.")
    return linhas


@task(log_prints=True)
def dispara_full_refresh_chatbot(
    deployment_name: str,
    select: str,
    github_repo: str,
    bigquery_project: str,
) -> None:
    """Dispara o full refresh do ramo Salesforce da chatbot (deployment do
    rj_crm__run_dbt) pra tema_nome/tema_motivo recalculados chegarem ao struct `llm` de
    todas as sessões, de qualquer data. Os incrementais da cadeia só reescrevem o passado
    recente (chatbot: ~9 dias; fct_chatbot_salesforce: 35), então sem isso o tema novo
    só aparece nas sessões novas.

    A pipeline não escreve na fato (o dbt a regrava a cada rodada — ver
    models/mart/chatbot/salesforce/DIVIDA_TECNICA.md, repo queries-rj-crm-registry). O
    select parte de int_chatbot_salesforce_sessao_receptivo, onde está o left join com a
    tabela de classificação, e fica fora do ramo da fct_chatbot_v2: um full refresh lá
    apagaria o llm do histórico dela, que casa pelos ids antigos do Agentforce.

    Espera o run terminar e falha se ele não completar — o deployment tem
    concurrency_limit 1 com ENQUEUE, então pode ficar na fila atrás do quarter_hourly."""
    flow_run = run_deployment(
        name=deployment_name,
        parameters={
            "command": "run",
            "select": select,
            "flag": "--full-refresh",
            "github_repo": github_repo,
            "bigquery_project": bigquery_project,
            "target": "prod",
            "gcs_buckets": {"prod": "rj-crm-registry_dbt", "dev": "rj-crm-registry-dev_dbt"},
        },
    )
    print(f"[TAXONOMIA] full refresh '{select}': flow run {flow_run.id} terminou em {flow_run.state.name}.")
    if not flow_run.state.is_completed():
        raise RuntimeError(
            f"Full refresh do dbt ({deployment_name}, select '{select}') não completou: "
            f"{flow_run.state.name}. tema_nome/tema_motivo já estão na tabela de classificação; "
            f"rodar o full refresh de novo pra eles chegarem à chatbot."
        )
