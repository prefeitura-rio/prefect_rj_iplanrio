# -*- coding: utf-8 -*-
"""
Aplicação das regras de tema/motivo (funções Python já induzidas e validadas no repo
`clustering`, tabela `taxonomia_regras`) sobre o `resumo` das sessões recém-classificadas.

Diferente da classificação inicial (etapa 1, com LLM), esta etapa não chama LLM nenhuma:
as funções já existem (foram induzidas por LLM sobre uma amostra, e validadas por precisão
em `clustering/validacao_taxonomia.ipynb` — ver docs/pipeline_clusterizacao_agentforce.md),
aqui só são executadas. Por isso cabe na mesma execução diária da classificação, sem custo
de API extra.

Catálogo (`taxonomia_regras`) é dado, não código: promovido manualmente pra lá depois da
validação de precisão (não existe promoção automática — função gerada por LLM não vira
produção sem humano no meio). Mesma tabela serve tema e motivo (coluna `etapa`); hoje só
tema tem linha.

`RulesSandbox` portado de clustering/modules/rules_sandbox.py — mesma lógica, sem mudança
de comportamento: valida o código via AST antes de compilar (bloqueia import fora de
re/unicodedata, eval/exec/open/os/subprocess, acesso a atributo dunder) e rejeita função
que dependa de algo fora dela mesma (garante que regra_python é autocontida).
"""

from __future__ import annotations

import ast
import re
import unicodedata
from typing import ClassVar

import pandas as pd
from google.api_core.exceptions import NotFound
from google.cloud import bigquery
from prefect import task

from pipelines.rj_crm__agentforce_classificacao_llm.utils.bigquery import get_bq_client

# Secretaria usada no catálogo (e no CSV de origem, clustering/data/categorias_taxonomia.csv)
# pra sessão relevante sem secretaria identificada pela LLM — secretaria_relacionada vem
# NULL da classificação inicial nesse caso, não essa string; o mapeamento é feito aqui.
_SECRETARIA_NAO_IDENTIFICADA = "Sem secretaria identificada"


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


@task(log_prints=True, retries=2, retry_delay_seconds=30)
def carrega_catalogo_regras(project_id: str, dataset_id: str, table_id: str, etapa: str) -> pd.DataFrame:
    """Lê as regras ativas de uma etapa (hoje só 'tema' tem linha; 'motivo' é o mesmo
    catálogo, só filtrando etapa diferente quando existir). Tabela é gerenciada por fora
    desta pipeline (promovida manualmente do clustering, ver docstring do módulo) — se
    ainda não existir, trata como "sem regras hoje" em vez de falhar o flow inteiro."""
    client = get_bq_client(project_id)
    full_id = f"{project_id}.{dataset_id}.{table_id}"
    query = f"""
        SELECT secretaria, categoria_pai, nome_funcao, nome, descricao, regra_python
        FROM `{full_id}`
        WHERE etapa = @etapa AND ativo_indicador = TRUE
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("etapa", "STRING", etapa)]
    )
    try:
        df = client.query(query, job_config=job_config).to_dataframe()
    except NotFound:
        print(f"[TAXONOMIA] '{table_id}' não existe ainda — nenhuma regra de '{etapa}' aplicada nesta execução.")
        return pd.DataFrame(columns=["secretaria", "categoria_pai", "nome_funcao", "nome", "descricao", "regra_python"])

    print(f"[TAXONOMIA] {len(df)} regra(s) ativa(s) de '{etapa}' carregadas do catálogo.")
    return df


@task(log_prints=True)
def aplica_regras_tema(df_final: pd.DataFrame, df_regras: pd.DataFrame) -> pd.DataFrame:
    """Roda cada regra do catálogo (já sandboxed) contra o `resumo` das sessões relevantes
    da secretaria correspondente, e preenche `tema_nome` com os nomes das categorias que
    bateram (pode ser mais de uma — por isso a coluna é array). Sessão não relevante, sem
    resumo, ou de secretaria sem regra no catálogo continua com `tema_nome` vazio (já é o
    default vindo de monta_dataframe_final).

    Regra que falha ao compilar (rejeitada pelo sandbox) ou ao rodar numa sessão específica
    é pulada e logada — nunca derruba a classificação da sessão nem do resto das regras."""
    if df_final.empty or df_regras.empty:
        return df_final

    sandbox = RulesSandbox()
    regras_por_secretaria: dict[str, list[dict]] = {}
    n_rejeitadas = 0
    for row in df_regras.itertuples():
        try:
            compiladas = sandbox.compile(row.regra_python)
        except ValueError as e:
            n_rejeitadas += 1
            print(f"[TAXONOMIA] regra '{row.nome_funcao}' (secretaria={row.secretaria}) rejeitada pelo sandbox: {e}")
            continue
        for info in compiladas.values():
            regras_por_secretaria.setdefault(row.secretaria, []).append(
                {"nome_categoria": row.nome, "funcao": info["funcao"]}
            )

    if not regras_por_secretaria:
        print(f"[TAXONOMIA] nenhuma regra compilou com segurança ({n_rejeitadas} rejeitada(s)) — tema_nome fica vazio.")
        return df_final

    def _aplica(row) -> list[str]:
        # nullable boolean (pd.NA) não aceita `not x` direto — comparação explícita
        if row.get("conteudo_relevante") is not True:
            return []
        resumo = row.get("resumo")
        if not resumo or (isinstance(resumo, float) and pd.isna(resumo)):
            return []

        secretaria = row.get("secretaria_relacionada") or _SECRETARIA_NAO_IDENTIFICADA
        regras = regras_por_secretaria.get(secretaria, [])
        matches = []
        for regra in regras:
            try:
                if regra["funcao"](resumo):
                    matches.append(regra["nome_categoria"])
            except Exception as e:  # função individual não pode derrubar a sessão inteira
                print(
                    f"[TAXONOMIA] falha ao avaliar '{regra['nome_categoria']}' "
                    f"na sessão {row.get('id_sessao')}: {type(e).__name__}: {e}"
                )
        return matches

    df_final = df_final.copy()
    df_final["tema_nome"] = df_final.apply(_aplica, axis=1)

    n_com_tema = (df_final["tema_nome"].str.len() > 0).sum()
    print(
        f"[TAXONOMIA] {n_rejeitadas} regra(s) rejeitada(s) pelo sandbox, "
        f"{sum(len(v) for v in regras_por_secretaria.values())} regra(s) aplicadas, "
        f"{n_com_tema} sessão(ões) com tema atribuído."
    )
    return df_final
