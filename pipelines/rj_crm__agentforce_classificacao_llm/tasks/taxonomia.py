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
from datetime import date, datetime, timezone
from typing import ClassVar

import pandas as pd
from google.api_core.exceptions import NotFound
from google.cloud import bigquery
from prefect import task

from pipelines.rj_crm__agentforce_classificacao_llm.tasks.load import SCHEMA, _full_table_id
from pipelines.rj_crm__agentforce_classificacao_llm.utils.bigquery import get_bq_client

# Secretaria usada no catálogo (e no CSV de origem, clustering/data/categorias_taxonomia.csv)
# pra sessão relevante sem secretaria identificada pela LLM — secretaria_principal vem
# NULL da classificação inicial nesse caso, não essa string; o mapeamento é feito aqui.
_SECRETARIA_NAO_IDENTIFICADA = "Sem secretaria identificada"


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


def _aplica_regras(df_final: pd.DataFrame, df_regras: pd.DataFrame, coluna_saida: str, rotulo_etapa: str) -> pd.DataFrame:
    """Lógica compartilhada entre aplica_regras_tema e aplica_regras_causa: roda cada
    regra do catálogo (já sandboxed) contra o `resumo` normalizado (ver
    `_normaliza_resumo`) das sessões relevantes da secretaria correspondente, e preenche
    `coluna_saida` com os nomes das categorias que bateram (pode ser mais de uma — por
    isso a coluna é array). Sessão não relevante, sem resumo, ou de secretaria sem regra
    no catálogo continua com `coluna_saida` vazio (já é o default vindo de
    monta_dataframe_final).

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
        print(
            f"[TAXONOMIA] nenhuma regra de '{rotulo_etapa}' compilou com segurança "
            f"({n_rejeitadas} rejeitada(s)) — {coluna_saida} fica vazio."
        )
        return df_final

    def _aplica(row) -> list[str]:
        # nullable boolean (pd.NA) não aceita `not x` direto — comparação explícita
        if row.get("conteudo_relevante") is not True:
            return []
        resumo = row.get("resumo")
        if not resumo or (isinstance(resumo, float) and pd.isna(resumo)):
            return []

        # secretaria_principal NULL no BigQuery chega aqui como float('nan'), não None —
        # e nan é truthy em Python (`nan or default` fica nan, não cai no default). Sem
        # o pd.isna aqui, sessão sem secretaria virava secretaria=nan, não achava nada em
        # regras_por_secretaria (indexado por string) e nunca recebia tema_nome/causa_nome
        # (bug real observado em produção: 0/1467 sessões "sem secretaria" classificadas).
        secretaria_bruta = row.get("secretaria_principal")
        if pd.isna(secretaria_bruta):
            secretaria_bruta = None
        secretaria = secretaria_bruta or _SECRETARIA_NAO_IDENTIFICADA
        regras = regras_por_secretaria.get(secretaria, [])
        matches = []
        resumo_norm = _normaliza_resumo(resumo)
        for regra in regras:
            try:
                if regra["funcao"](resumo_norm):
                    matches.append(regra["nome_categoria"])
            except Exception as e:  # função individual não pode derrubar a sessão inteira
                print(
                    f"[TAXONOMIA] falha ao avaliar '{regra['nome_categoria']}' "
                    f"na sessão {row.get('id_sessao')}: {type(e).__name__}: {e}"
                )
        return matches

    df_final = df_final.copy()
    df_final[coluna_saida] = df_final.apply(_aplica, axis=1)

    n_com_categoria = (df_final[coluna_saida].str.len() > 0).sum()
    print(
        f"[TAXONOMIA] {n_rejeitadas} regra(s) de '{rotulo_etapa}' rejeitada(s) pelo sandbox, "
        f"{sum(len(v) for v in regras_por_secretaria.values())} regra(s) aplicadas, "
        f"{n_com_categoria} sessão(ões) com {coluna_saida} atribuído."
    )
    return df_final


@task(log_prints=True)
def aplica_regras_tema(df_final: pd.DataFrame, df_regras: pd.DataFrame) -> pd.DataFrame:
    """Etapa 2 (tema, por secretaria) — ver _aplica_regras pra lógica completa."""
    return _aplica_regras(df_final, df_regras, coluna_saida="tema_nome", rotulo_etapa="tema")


@task(log_prints=True)
def aplica_regras_causa(df_final: pd.DataFrame, df_regras: pd.DataFrame) -> pd.DataFrame:
    """Etapa 3 (causa sistêmica/motivo) — mesma lógica de aplica_regras_tema (mesmo
    sandbox, mesmo escopo por secretaria, mesma avaliação contra `resumo`), só que
    escreve em causa_nome e filtra o catálogo por etapa='motivo' (ver
    TAXONOMIA_ETAPA_MOTIVO em constants.py). Hoje o catálogo não tem nenhuma regra
    dessa etapa — chamar esta função com df_regras vazio é seguro e não altera
    causa_nome (early-return em _aplica_regras). Assume o mesmo modelo de escopo do
    tema (por secretaria); se a indução de causa acabar sendo escopada dentro do tema
    em vez de por secretaria, revisar aqui quando a primeira regra real for promovida."""
    return _aplica_regras(df_final, df_regras, coluna_saida="causa_nome", rotulo_etapa="motivo")


# ---------------------------------------------------------------------------
# Recálculo de taxonomia (parâmetro recalcula_taxonomia do flow) — ver flow.py.
#
# Cenário: a taxonomia (catálogo de regras) muda — uma regra é corrigida, uma
# categoria é adicionada — e as sessões já classificadas precisam refletir isso,
# sem rechamar a LLM (custo zero, é só reavaliar função Python contra o resumo
# que já está salvo) e sem sobrescrever relacao_hsm/resumo/motivo/etc. (só
# tema_nome/causa_nome mudam). Opção A discutida: MERGE parcial (só essas 2
# colunas) direto na tabela auxiliar E nas tabelas mart do dbt (chatbot/
# v2_chatbot_conversas), que são incrementais e não reprocessam retroativamente
# sozinhas quando a fonte (auxiliar) muda.
# ---------------------------------------------------------------------------


@task(log_prints=True, retries=2, retry_delay_seconds=30)
def extrai_sessoes_para_recalculo_taxonomia(project_id: str, dataset_id: str, table_id: str) -> pd.DataFrame:
    """Lê da própria tabela destino (não da fonte, não rechama a LLM) só o que
    aplica_regras_tema precisa: id_sessao, resumo, secretaria_principal,
    conteudo_relevante — mesmo filtro (conteudo_relevante = true) que a etapa 1 já usa
    internamente pra decidir quem entra na avaliação de regra. Inclui causa_nome já
    existente (não recalculado aqui — aplica_regras_tema só sabe recalcular tema; sem
    isso, atualiza_tema_causa reescreveria causa_nome como vazio à toa)."""
    client = get_bq_client(project_id)
    full_id = _full_table_id(project_id, dataset_id, table_id)
    query = f"""
        SELECT id_sessao, resumo, secretaria_principal, conteudo_relevante, causa_nome
        FROM `{full_id}`
        WHERE conteudo_relevante = true
    """
    df = client.query(query).to_dataframe()
    print(f"[TAXONOMIA] {len(df)} sessão(ões) relevante(s) carregada(s) da tabela destino pra recálculo de tema/causa.")
    return df


@task(log_prints=True, retries=3, retry_delay_seconds=[30, 60, 120])
def atualiza_tema_causa(
    df_tema: pd.DataFrame,
    project_id: str,
    dataset_id: str,
    table_id: str,
    tmp_table_id: str,
) -> int:
    """MERGE PARCIAL — atualiza só tema_nome/causa_nome (por id_sessao) na tabela
    destino, sem tocar em nenhuma outra coluna (relacao_hsm, resumo, motivo etc.
    ficam intactos). Diferente de carrega_classificacoes (tasks/load.py), que
    sobrescreve a linha inteira — esta função é exatamente o "UPDATE parcial,
    coluna a coluna" que o docstring daquela função avisa ser necessário aqui.

    Reaproveita a mesma tabela tmp da carga normal (schema físico já tem as REQUIRED
    id_sessao/classificado_em/data_particao) — como o MERGE só faz UPDATE SET nas 2
    colunas de tema/causa, os valores de classificado_em/data_particao carregados
    aqui são só placeholder pra satisfazer o schema da tmp: nunca chegam na tabela
    destino."""
    if df_tema.empty:
        print("[TAXONOMIA] Nada a atualizar — nenhuma sessão com tema/causa recalculado.")
        return 0

    client = get_bq_client(project_id)
    tmp_full = _full_table_id(project_id, dataset_id, tmp_table_id)
    destino_full = _full_table_id(project_id, dataset_id, table_id)

    agora = datetime.now(tz=timezone.utc)
    df_carga = df_tema[["id_sessao", "tema_nome", "causa_nome"]].copy()
    df_carga["classificado_em"] = agora  # placeholder, nunca lido pelo MERGE (ver docstring)
    df_carga["data_particao"] = date.today()  # placeholder, idem

    colunas_tmp = {"id_sessao", "tema_nome", "causa_nome", "classificado_em", "data_particao"}
    schema_tmp = [f for f in SCHEMA if f.name in colunas_tmp]

    job_config = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_APPEND, schema=schema_tmp)
    job = client.load_table_from_dataframe(df_carga, tmp_full, job_config=job_config)
    job.result()
    if job.errors:
        raise RuntimeError(f"[TAXONOMIA] Erro ao carregar tmp: {job.errors}")

    merge_sql = f"""
        MERGE `{destino_full}` AS t
        USING `{tmp_full}` AS s
        ON t.id_sessao = s.id_sessao
        WHEN MATCHED THEN
            UPDATE SET t.tema_nome = s.tema_nome, t.causa_nome = s.causa_nome
    """
    merge_job = client.query(merge_sql)
    merge_job.result()
    linhas = merge_job.num_dml_affected_rows or 0

    client.query(f"TRUNCATE TABLE `{tmp_full}`").result()
    print(f"[TAXONOMIA] MERGE parcial concluído: {linhas} linha(s) de tema/causa atualizada(s) em '{table_id}'.")
    return linhas


@task(log_prints=True, retries=3, retry_delay_seconds=[30, 60, 120])
def propaga_tema_causa_chatbot(
    project_id: str,
    aux_full_table_id: str,
    chatbot_dataset_id: str,
    chatbot_v1_table_id: str,
    chatbot_v2_table_id: str,
) -> int:
    """Propaga tema_nome/causa_nome (já atualizados na tabela auxiliar por
    atualiza_tema_causa) pras tabelas mart do dbt — chatbot (v1) e v2_chatbot_conversas
    (v2). Necessário porque essas tabelas são incrementais (insert_overwrite): não
    reprocessam retroativamente sozinhas só porque a fonte (tabela auxiliar) mudou —
    sem isso, ficariam com o tema antigo até alguém rodar um `dbt run --full-refresh`
    (caro: reconstrói a tabela inteira só pra atualizar 2 colunas).

    Mesma lógica de MERGE parcial de atualiza_tema_causa, só que a fonte aqui já é a
    tabela auxiliar (não precisa de tmp: não é um load de DataFrame, é MERGE direto
    entre 2 tabelas que já existem no BigQuery)."""
    client = get_bq_client(project_id)
    linhas_total = 0
    for table_id in (chatbot_v1_table_id, chatbot_v2_table_id):
        chatbot_full = f"{project_id}.{chatbot_dataset_id}.{table_id}"
        merge_sql = f"""
            MERGE `{chatbot_full}` AS t
            USING `{aux_full_table_id}` AS s
            ON t.id_interacao = s.id_sessao
            WHEN MATCHED THEN
                UPDATE SET t.tema_nome = s.tema_nome, t.causa_nome = s.causa_nome
        """
        merge_job = client.query(merge_sql)
        merge_job.result()
        linhas = merge_job.num_dml_affected_rows or 0
        linhas_total += linhas
        print(f"[TAXONOMIA] '{table_id}': {linhas} linha(s) de tema/causa propagada(s).")

    return linhas_total
