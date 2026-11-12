# -*- coding: utf-8 -*-
"""
Taxonomias dos prompts de roteamento (macrotema e secretaria), lidas do BigQuery.

As tabelas brutos_salesforce.taxonomia_macrotemas e taxonomia_secretarias são a fonte
única: viram a lista renderizada nos prompts e o catálogo de rótulos válidos na
validação da resposta da LLM. São editadas à mão (raramente) e também alimentam o painel
de clusterização no dbt.

Cada tabela traz a versão do seu prompt (prompt_versao_macrotema,
prompt_versao_secretaria), gravada em cada sessão classificada: quem edita a taxonomia
de forma que muda o significado da classificação sobe a versão na mesma edição. Todas as
linhas da tabela têm a mesma versão.

A ordem das listas (coluna `ordem`) é a do prompt: um prefixo sempre igual aproveita o
cache implícito de contexto do Gemini.

Histórico de versões:
- macrotema v1 (2026-10-21): macrotemas da planilha de taxonomia (aba Macrotemas) +
"Outros". v2 (2026-11-03): nomes sem vírgula nem " e " (viraram " / ", ex.: "Trabalho /
renda / qualificação"); os relacionados são separados por vírgula, e nome com vírgula
era quebrado na validação e descartado. As sessões da v1 foram renomeadas por UPDATE.
- secretaria v1 (2026-10-21): secretarias da aba Núcleo, mantendo a grafia do catálogo
taxonomia_regras para as 7 secretarias originais (Comlurb, RioLuz, Seconserva...). v2
(2026-11-03): SUBGGC entra para remuneração e vida funcional do servidor ativo, e a
PREVI-RIO fica só com previdência e os auxílios do instituto (sessões da v1 migradas por
UPDATE). v3 (2026-11-03): o prompt lista só as secretarias candidatas dos macrotemas da
sessão (coluna `secretarias` da taxonomia_macrotemas), com "OUTRA" refazendo a pergunta
com a lista completa.
- 2026-11-11: as listas saem de taxonomias/*.json para as tabelas, sem mudar conteúdo
nem versão.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from prefect import task

from pipelines.rj_crm__agentforce_classificacao_llm.utils.bigquery import get_bq_client

_PROMPTS_DIR = Path(__file__).resolve().parent.parent / "prompts"
_TEMPLATE_MACROTEMA_BASE = (_PROMPTS_DIR / "classificacao_macrotema.txt").read_text()
_TEMPLATE_SECRETARIA_BASE = (_PROMPTS_DIR / "classificacao_secretaria.txt").read_text()

# Secretaria: o prompt mostra só as candidatas dos macrotemas da sessão — a lista inteira
# tem 63 órgãos e era metade dos tokens de uma sessão relevante. Macrotema sem lista
# ("Outros") ou ausente usa a lista completa. Na lista filtrada a LLM pode responder
# "OUTRA" (órgão da Prefeitura fora das candidatas), e aí a pergunta é refeita com a
# lista completa.
SECRETARIA_FORA_DA_LISTA = "OUTRA"
_REGRA_FORA_DA_LISTA_COMPLETA = (
    "3. Se o assunto não for responsabilidade de nenhum órgão da lista (ex.: CEDAE, Light, Detran, "
    "INSS, Governo do Estado), os dois campos devem ser null."
)
_REGRA_FORA_DA_LISTA_FILTRADA = (
    "3. A lista abaixo traz só os órgãos ligados ao assunto desta conversa. Se o assunto for de um "
    f'órgão da Prefeitura que não está nela, responda "{SECRETARIA_FORA_DA_LISTA}" nos dois campos. '
    "Se não for responsabilidade de nenhum órgão da Prefeitura (ex.: CEDAE, Light, Detran, INSS, "
    "Governo do Estado), os dois campos devem ser null."
)


@dataclass
class TaxonomiaRoteamento:
    """Macrotemas e secretarias, na ordem do prompt, com as versões dos dois prompts.
    macrotemas: dicts com nome, descricao, secretarias; secretarias: dicts com sigla,
    nome, descricao."""

    macrotemas: list[dict]
    secretarias: list[dict]
    prompt_versao_macrotema: str
    prompt_versao_secretaria: str
    _templates_secretaria: dict = field(default_factory=dict, repr=False)

    def __post_init__(self) -> None:
        # Os relacionados chegam da LLM e são gravados separados por vírgula — rótulo com
        # vírgula seria quebrado em pedaços que não batem com o catálogo e descartado.
        com_virgula = [m["nome"] for m in self.macrotemas if "," in m["nome"]] + [
            s["sigla"] for s in self.secretarias if "," in s["sigla"]
        ]
        if com_virgula:
            raise ValueError(f"rótulo de taxonomia com vírgula (separador dos relacionados): {com_virgula}")
        siglas = {s["sigla"] for s in self.secretarias}
        desconhecidas = {s for m in self.macrotemas for s in (m.get("secretarias") or [])} - siglas
        if desconhecidas:
            raise ValueError(
                f"taxonomia_macrotemas cita sigla fora de taxonomia_secretarias: {sorted(desconhecidas)}"
            )
        self.macrotemas_validos = [m["nome"] for m in self.macrotemas]
        self.secretarias_validas = [s["sigla"] for s in self.secretarias]
        self.secretarias_por_macrotema = {
            m["nome"]: list(m["secretarias"]) for m in self.macrotemas if m.get("secretarias")
        }
        # renderizado uma vez: fica no começo do prompt e é idêntico em toda chamada
        self.template_macrotema = _TEMPLATE_MACROTEMA_BASE.replace(
            "<<MACROTEMAS>>", "\n".join(f"- {m['nome']}: {m['descricao']}" for m in self.macrotemas)
        )

    def template_secretaria(self, candidatas: tuple[str, ...] | None) -> str:
        """Template de secretaria com a lista de órgãos já renderizada — completa (None) ou
        só as candidatas, na ordem do catálogo: o mesmo conjunto gera sempre o mesmo
        prefixo, que o cache implícito do Gemini aproveita. Memoizado por conjunto (as
        threads de classifica_sessoes só leem e gravam o mesmo valor)."""
        if candidatas not in self._templates_secretaria:
            if candidatas is None:
                orgaos, regra = self.secretarias, _REGRA_FORA_DA_LISTA_COMPLETA
            else:
                orgaos = [s for s in self.secretarias if s["sigla"] in candidatas]
                regra = _REGRA_FORA_DA_LISTA_FILTRADA
            self._templates_secretaria[candidatas] = _TEMPLATE_SECRETARIA_BASE.replace(
                "<<SECRETARIAS>>", "\n".join(f"- {s['sigla']} – {s['nome']}: {s['descricao']}" for s in orgaos)
            ).replace("<<REGRA_FORA_DA_LISTA>>", regra)
        return self._templates_secretaria[candidatas]

    def secretarias_candidatas(self, macrotemas_relacionados) -> tuple[str, ...] | None:
        """União das secretarias dos macrotemas da sessão (relacionados já inclui o
        principal). None = lista completa: sem macrotema, ou algum deles sem lista própria."""
        if not macrotemas_relacionados:
            return None
        candidatas: set[str] = set()
        for nome in macrotemas_relacionados.split(", "):
            if nome not in self.secretarias_por_macrotema:
                return None
            candidatas.update(self.secretarias_por_macrotema[nome])
        return tuple(sorted(candidatas))


def _versao_unica(df, coluna: str, tabela: str) -> str:
    versoes = sorted(df[coluna].dropna().unique())
    if len(versoes) != 1:
        raise ValueError(f"{tabela} precisa de exatamente uma {coluna} em todas as linhas, achou: {versoes}")
    return versoes[0]


@task(log_prints=True, retries=2, retry_delay_seconds=30)
def carrega_taxonomia_roteamento(
    project_id: str, dataset_id: str, macrotemas_table_id: str, secretarias_table_id: str
) -> TaxonomiaRoteamento:
    """Lê as duas tabelas, em ordem, e monta a TaxonomiaRoteamento. Tabela vazia ou com
    versões diferentes falha o flow: sem taxonomia não há como rotear."""
    client = get_bq_client(project_id)
    df_mt = client.query(
        f"""
        SELECT nome, descricao, secretarias, prompt_versao_macrotema
        FROM `{project_id}.{dataset_id}.{macrotemas_table_id}`
        ORDER BY ordem
        """
    ).to_dataframe()
    df_sec = client.query(
        f"""
        SELECT sigla, nome, descricao, prompt_versao_secretaria
        FROM `{project_id}.{dataset_id}.{secretarias_table_id}`
        ORDER BY ordem
        """
    ).to_dataframe()
    if df_mt.empty or df_sec.empty:
        raise ValueError(
            f"taxonomia vazia: {len(df_mt)} macrotema(s) em '{macrotemas_table_id}', "
            f"{len(df_sec)} secretaria(s) em '{secretarias_table_id}'."
        )
    taxonomia = TaxonomiaRoteamento(
        macrotemas=[
            {"nome": r.nome, "descricao": r.descricao, "secretarias": list(r.secretarias)}
            for r in df_mt.itertuples()
        ],
        secretarias=[{"sigla": r.sigla, "nome": r.nome, "descricao": r.descricao} for r in df_sec.itertuples()],
        prompt_versao_macrotema=_versao_unica(df_mt, "prompt_versao_macrotema", macrotemas_table_id),
        prompt_versao_secretaria=_versao_unica(df_sec, "prompt_versao_secretaria", secretarias_table_id),
    )
    print(
        f"[ROTEAMENTO] {len(taxonomia.macrotemas)} macrotema(s) ({taxonomia.prompt_versao_macrotema}) e "
        f"{len(taxonomia.secretarias)} secretaria(s) ({taxonomia.prompt_versao_secretaria}) carregados."
    )
    return taxonomia
