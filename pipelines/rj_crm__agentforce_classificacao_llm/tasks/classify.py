# -*- coding: utf-8 -*-
"""
Classificação por LLM (Bifrost/Gemini) das sessões do Agentforce.

Portado de clustering/classificacao_inicial.ipynb (seção 5) e clustering/modules/bifrost.py
+ clustering/modules/llm_responses.py + clustering/modules/prompts.py, com uma mudança:
o cliente HTTP troca `curl` via subprocess por `requests` (já é dependência padrão no
resto deste monorepo — ver pipelines/rj_crm__wetalkie_api_hsm_info, tasks/notify.py etc.),
mantendo o mesmo retry/backoff.

Regra de negócio (não muda em produção): sessão sem HSM associado -> prompt "sem_hsm";
sessão com HSM e não marcada como resposta_atrasada_btn -> prompt "com_hsm"; sessão sem
nenhuma mensagem do usuário -> descartada (nunca é classificada, igual ao notebook).
"""

from __future__ import annotations

import functools
import json
import re
import time
import unicodedata
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests
from prefect import task

from pipelines.rj_crm__agentforce_classificacao_llm.tasks.load import carrega_classificacoes
from pipelines.rj_crm__agentforce_classificacao_llm.tasks.taxonomia import aplica_regras_causa, aplica_regras_tema

_PROMPTS_DIR = Path(__file__).resolve().parent.parent / "prompts"
_TAXONOMIAS_DIR = Path(__file__).resolve().parent.parent / "taxonomias"

# Colunas de metadado da sessão (vindas da extração/enriquecimento) que precisam
# sobreviver até a tabela destino — tanto pras sessões que passam pela LLM quanto
# pras pré-classificadas por regra. _processa (em classifica_sessoes) não as
# recebe: ela só sabe o que a LLM devolveu, então elas voltam via merge por
# id_sessao depois. hsm_texto/conversa_completa entram aqui pra permitir validar a
# classificação (resumo/motivo/tema) lendo a conversa original direto da tabela
# destino, sem precisar reconstruir a partir de prompt_enviado (que é null nas
# sessões RESPOSTA_ATRASADA_BTN, decididas por regra sem LLM). Note que
# mensagens_usuario_concatenadas (só cidadão) NÃO está aqui — não persiste nem vai
# pro prompt, é redundante com conversa_completa (cidadão + agente, rotulado), que a
# substituiu como contexto pra LLM; mensagens_usuario_concatenadas continua existindo
# só internamente no dataframe da extração, usada pelo filtro tem_conversa em
# monta_prompts. jornada_nome/id_jornada/hsm_envio_datahora vêm nulos desde que a
# sessão passou a ser id_sessao_receptivo (o HSM fica em id_disparo_hsm), e hsm_texto
# é o texto renderizado pelo dbt (receptivo_hsm_texto_renderizado).
_COLUNAS_METADADO_SESSAO = [
    "id_sessao", "telefone", "telefone_original", "cpf", "nome_cidadao", "sessao_inicio_datahora",
    "sessao_fim_datahora", "jornada_nome", "id_jornada", "id_disparo_hsm", "hsm_envio_datahora",
    "hsm_texto", "conversa_completa",
]
_TEMPLATE_COM_HSM = (_PROMPTS_DIR / "classificacao_hsm.txt").read_text()
_TEMPLATE_SEM_HSM = (_PROMPTS_DIR / "classificacao_sem_hsm.txt").read_text()

# Taxonomias dos prompts de roteamento (macrotema, secretaria) — o JSON é a fonte única:
# vira a lista renderizada no prompt e o catálogo de rótulos válidos na validação
# (_MACROTEMAS_VALIDOS/_SECRETARIAS_VALIDAS, mais abaixo), então editar o JSON já muda
# os dois juntos. Renderizada uma vez aqui (não por sessão): fica no começo do prompt,
# antes da conversa, e é idêntica em toda chamada — prefixo estável aproveita o cache
# implícito de contexto do Gemini.
_MACROTEMAS = json.loads((_TAXONOMIAS_DIR / "macrotemas.json").read_text())
_SECRETARIAS = json.loads((_TAXONOMIAS_DIR / "secretarias.json").read_text())
# Os relacionados chegam da LLM e são gravados separados por vírgula — rótulo com
# vírgula seria quebrado em pedaços que não batem com o catálogo e descartado.
_ROTULOS_COM_VIRGULA = [m["nome"] for m in _MACROTEMAS if "," in m["nome"]] + [
    s["sigla"] for s in _SECRETARIAS if "," in s["sigla"]
]
if _ROTULOS_COM_VIRGULA:
    raise ValueError(f"rótulo de taxonomia com vírgula (separador dos relacionados): {_ROTULOS_COM_VIRGULA}")
_TEMPLATE_MACROTEMA = (_PROMPTS_DIR / "classificacao_macrotema.txt").read_text().replace(
    "<<MACROTEMAS>>", "\n".join(f"- {m['nome']}: {m['descricao']}" for m in _MACROTEMAS)
)
# Secretaria: o prompt mostra só as candidatas dos macrotemas da sessão (campo
# "secretarias" de cada macrotema no macrotemas.json) — a lista inteira tem 63 órgãos e
# era metade dos tokens de uma sessão relevante. Macrotema sem lista ("Outros") ou
# ausente usa a lista completa. Na lista filtrada a LLM pode responder "OUTRA" (órgão da
# Prefeitura fora das candidatas), e aí a pergunta é refeita com a lista completa.
_TEMPLATE_SECRETARIA_BASE = (_PROMPTS_DIR / "classificacao_secretaria.txt").read_text()
_SECRETARIA_FORA_DA_LISTA = "OUTRA"
_REGRA_FORA_DA_LISTA_COMPLETA = (
    "3. Se o assunto não for responsabilidade de nenhum órgão da lista (ex.: CEDAE, Light, Detran, "
    "INSS, Governo do Estado), os dois campos devem ser null."
)
_REGRA_FORA_DA_LISTA_FILTRADA = (
    "3. A lista abaixo traz só os órgãos ligados ao assunto desta conversa. Se o assunto for de um "
    f'órgão da Prefeitura que não está nela, responda "{_SECRETARIA_FORA_DA_LISTA}" nos dois campos. '
    "Se não for responsabilidade de nenhum órgão da Prefeitura (ex.: CEDAE, Light, Detran, INSS, "
    "Governo do Estado), os dois campos devem ser null."
)
_SECRETARIAS_POR_MACROTEMA = {m["nome"]: m["secretarias"] for m in _MACROTEMAS if m.get("secretarias")}
_SIGLAS_DESCONHECIDAS = {s for v in _SECRETARIAS_POR_MACROTEMA.values() for s in v} - {
    s["sigla"] for s in _SECRETARIAS
}
if _SIGLAS_DESCONHECIDAS:
    raise ValueError(f"macrotemas.json cita sigla fora de secretarias.json: {sorted(_SIGLAS_DESCONHECIDAS)}")


@functools.lru_cache(maxsize=None)
def _template_secretaria(candidatas: tuple[str, ...] | None) -> str:
    """Template de secretaria com a lista de órgãos já renderizada — completa (None) ou só
    as candidatas, na ordem do catálogo: o mesmo conjunto gera sempre o mesmo prefixo,
    que o cache implícito do Gemini aproveita."""
    if candidatas is None:
        orgaos, regra = _SECRETARIAS, _REGRA_FORA_DA_LISTA_COMPLETA
    else:
        orgaos, regra = [s for s in _SECRETARIAS if s["sigla"] in candidatas], _REGRA_FORA_DA_LISTA_FILTRADA
    return _TEMPLATE_SECRETARIA_BASE.replace(
        "<<SECRETARIAS>>", "\n".join(f"- {s['sigla']} – {s['nome']}: {s['descricao']}" for s in orgaos)
    ).replace("<<REGRA_FORA_DA_LISTA>>", regra)


def _secretarias_candidatas(macrotemas_relacionados) -> tuple[str, ...] | None:
    """União das secretarias dos macrotemas da sessão (relacionados já inclui o
    principal). None = lista completa: sem macrotema, ou algum deles sem lista própria."""
    if not macrotemas_relacionados:
        return None
    candidatas: set[str] = set()
    for nome in macrotemas_relacionados.split(", "):
        if nome not in _SECRETARIAS_POR_MACROTEMA:
            return None
        candidatas.update(_SECRETARIAS_POR_MACROTEMA[nome])
    return tuple(sorted(candidatas))
# Texto que entra no lugar do HSM nos prompts de roteamento quando a sessão é sem_hsm —
# eles são um template só pros dois casos (diferente do prompt principal, que tem
# com_hsm/sem_hsm separados porque sem HSM não há relacao_hsm/telefone pra avaliar).
_HSM_AUSENTE = "(Nenhum HSM identificado para esta sessão.)"


# ---------------------------------------------------------------------------
# Template rendering — portado de clustering/modules/prompts.py (PromptTemplate)
# ---------------------------------------------------------------------------


def _render(template: str, **campos) -> str:
    """Substitui cada <<CHAVE>> pelo valor correspondente. Valores None/NaN viram
    string vazia, em vez de "None"/"nan" no prompt."""
    texto = template
    for chave, valor_bruto in campos.items():
        valor = "" if valor_bruto is None or (isinstance(valor_bruto, float) and pd.isna(valor_bruto)) else valor_bruto
        texto = texto.replace(f"<<{chave.upper()}>>", str(valor))
    return texto


# ---------------------------------------------------------------------------
# Parsing da resposta — portado de clustering/modules/llm_responses.py
# ---------------------------------------------------------------------------


def _strip_code_fence(texto: str) -> str:
    texto = texto.strip()
    if texto.startswith("```"):
        texto = re.sub(r"^```(\w+)?\s*|\s*```$", "", texto, flags=re.IGNORECASE).strip()
    return texto


def _parse_json_response(texto: str) -> dict:
    return json.loads(_strip_code_fence(texto), strict=False)


# ---------------------------------------------------------------------------
# Sentimento — derivado por REGRA (não pela LLM) a partir de `natureza`, que
# essa sim vem da LLM. Ordem de prioridade fixa: Reclamação/Problema pesa mais
# que Elogio (uma sessão pode ter os dois — ex.: elogia uma coisa e reclama de
# outra — e aqui conta como negativo). Cai em neutro só quando TEM natureza mas
# nenhum dos dois (Dúvida/Solicitação/Informação isoladas). Sem natureza (null —
# caso das sessões RESPOSTA_ATRASADA_BTN, que não passam pela LLM, ou
# conteudo_relevante=false), sentimento também fica null — não é "sem opinião
# forte", é "não avaliado".
# ---------------------------------------------------------------------------


def _deriva_sentimento(natureza):
    # pd.isna() é essencial aqui: natureza ausente chega como float('nan'), não
    # None — e `not float('nan')` é False em Python (nan é truthy), então um
    # "if not natureza" simples deixaria passar o nan pro `in` abaixo e
    # estouraria TypeError. Visto em produção (run 2026-09-01, ~50 sessões
    # RESPOSTA_ATRASADA_BTN sem natureza no mesmo chunk).
    if pd.isna(natureza) or not natureza:
        return None
    if "Reclamação" in natureza or "Problema" in natureza:
        return "negativo"
    if "Elogio" in natureza:
        return "positivo"
    return "neutro"


# ---------------------------------------------------------------------------
# Cliente Bifrost — portado de clustering/modules/bifrost.py, curl -> requests
# ---------------------------------------------------------------------------


class BifrostClient:
    """Cliente HTTP pro gateway Bifrost (Vertex/Gemini), com retry de backoff
    exponencial para falha transitória (rate limit 429, 5xx, timeout de rede)."""

    def __init__(self, api_key: str, base: str, model: str, timeout: int = 60):
        self.api_key = api_key
        self.base = base
        self.model = model
        self.timeout = timeout
        self._session = requests.Session()

    def ask(
        self,
        prompt: str,
        system_instruction: str = "Responda em PT-BR.",
        max_output_tokens: int = 2048,
        temperature: float = 0.0,
        thinking_budget: int = 0,
        max_tentativas: int = 3,
        espera_inicial: int = 2,
    ) -> dict:
        payload = {
            "systemInstruction": {"parts": [{"text": system_instruction}]},
            "contents": [{"role": "user", "parts": [{"text": prompt}]}],
            "generationConfig": {
                "maxOutputTokens": max_output_tokens,
                "temperature": temperature,
                "thinkingConfig": {"thinkingBudget": thinking_budget, "includeThoughts": True},
            },
        }

        ultimo_erro = None
        for tentativa in range(1, max_tentativas + 1):
            try:
                return self._chama(payload)
            except RuntimeError as e:
                ultimo_erro = e
                if tentativa < max_tentativas:
                    espera = espera_inicial * (2 ** (tentativa - 1))
                    time.sleep(espera)
        raise ultimo_erro

    def _chama(self, payload: dict) -> dict:
        url = f"{self.base}/genai/v1beta/models/{self.model}:generateContent"
        headers = {"Content-Type": "application/json", "x-goog-api-key": self.api_key}
        try:
            resp = self._session.post(url, headers=headers, json=payload, timeout=self.timeout)
        except requests.RequestException as e:
            raise RuntimeError(f"requisição ao Bifrost falhou: {e}") from e

        # resposta de erro HTTP (429/5xx) pode vir com corpo JSON de erro; trata os dois casos
        try:
            parsed = resp.json()
        except ValueError as e:
            resp.raise_for_status()
            raise RuntimeError(f"Bifrost retornou corpo não-JSON (status {resp.status_code})") from e

        if isinstance(parsed, dict) and "error" in parsed:
            erro = parsed["error"]
            raise RuntimeError(
                f"Bifrost/API retornou erro {erro.get('code')} ({erro.get('status')}): {erro.get('message')}"
            )

        resp.raise_for_status()
        return parsed

    @staticmethod
    def extract_text(response: dict) -> str:
        parts = response["candidates"][0]["content"]["parts"]
        return "".join(p["text"] for p in parts if not p.get("thought"))


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------


@task(log_prints=True)
def monta_prompts(df_enriquecido: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Separa as sessões em 3 grupos (igual ao notebook):
      - pré-classificadas (resposta_atrasada_btn): não vão pra LLM;
      - com_hsm: tem hsm_texto e não é resposta atrasada;
      - sem_hsm: não tem hsm_texto.
    Sessão sem nenhuma mensagem do usuário é descartada dos 3 grupos (nada a classificar) —
    ela continua "pendente" e será extraída de novo nas próximas execuções, dentro da
    janela de LOOKBACK_DAYS, sem custo de LLM.

    Retorna (df_prompts, df_pre_classificadas).
    """
    if df_enriquecido.empty:
        return (
            pd.DataFrame(columns=["id_sessao", "tipo_prompt", "prompt"]),
            pd.DataFrame(columns=_COLUNAS_METADADO_SESSAO),
        )

    tem_hsm = df_enriquecido["hsm_texto"].notna() & (df_enriquecido["hsm_texto"].astype(str).str.strip() != "")
    tem_conversa = df_enriquecido["mensagens_usuario_concatenadas"].notna() & (
        df_enriquecido["mensagens_usuario_concatenadas"].astype(str).str.strip() != ""
    )

    enviar_llm_com_hsm = tem_hsm & ~df_enriquecido["resposta_atrasada_btn"] & tem_conversa
    pre_classificadas = tem_hsm & df_enriquecido["resposta_atrasada_btn"]
    enviar_llm_sem_hsm = ~tem_hsm & tem_conversa

    df_com_hsm = df_enriquecido.loc[enviar_llm_com_hsm].copy()
    df_com_hsm["tipo_prompt"] = "com_hsm"
    df_com_hsm["prompt"] = df_com_hsm.apply(
        lambda row: _render(
            _TEMPLATE_COM_HSM,
            hsm_texto=row["hsm_texto"],
            conversa_completa=row["conversa_completa"],
        ),
        axis=1,
    )

    df_sem_hsm = df_enriquecido.loc[enviar_llm_sem_hsm].copy()
    df_sem_hsm["tipo_prompt"] = "sem_hsm"
    df_sem_hsm["prompt"] = df_sem_hsm.apply(
        lambda row: _render(
            _TEMPLATE_SEM_HSM,
            conversa_completa=row["conversa_completa"],
        ),
        axis=1,
    )

    df_prompts = pd.concat([df_com_hsm, df_sem_hsm], ignore_index=True)

    df_pre_classificadas = df_enriquecido.loc[pre_classificadas, _COLUNAS_METADADO_SESSAO].copy()

    n_descartadas = len(df_enriquecido) - len(df_prompts) - len(df_pre_classificadas)
    n_com_hsm = (df_prompts["tipo_prompt"] == "com_hsm").sum()
    n_sem_hsm = (df_prompts["tipo_prompt"] == "sem_hsm").sum()
    print(
        f"[CLASSIFY] {len(df_prompts)} sessão(ões) pra LLM ({n_com_hsm} com_hsm / {n_sem_hsm} sem_hsm), "
        f"{len(df_pre_classificadas)} pré-classificada(s) por regra, "
        f"{n_descartadas} descartada(s) por não ter mensagem do usuário."
    )
    return df_prompts, df_pre_classificadas


# ---------------------------------------------------------------------------
# Validação dos rótulos que a LLM devolve — vistos em produção (2026-09-02): grafias
# fora do catálogo oficial do prompt ("Solicitacao" sem acento, "Reclamacao" sem acento)
# e até rótulo inventado, fora da lista inteira ("Dificuldade", nunca pedido no prompt).
# As funções abaixo comparam sem acento/caixa contra o catálogo oficial — corrige a
# grafia de volta pra canônica quando bate, descarta (fica de fora) quando não bate com
# nada. Preferível a manter o valor bruto da LLM: um rótulo fora do catálogo quebraria
# contagem/agrupamento por valor exato em qualquer análise a jusante, silenciosamente.
# ---------------------------------------------------------------------------
_RELACAO_HSM_VALIDAS = ["DENTRO_DO_ESCOPO", "FORA_DO_ESCOPO", "MISTO"]
_NATUREZA_VALIDAS = ["Dúvida", "Problema", "Reclamação", "Elogio", "Solicitação", "Informação"]
_SECRETARIAS_VALIDAS = [s["sigla"] for s in _SECRETARIAS]
_MACROTEMAS_VALIDOS = [m["nome"] for m in _MACROTEMAS]
# telefone_confirmacao_flag: item 7 do prompt com_hsm, ver docstring de monta_dataframe_final
# pro caso RESPOSTA_ATRASADA_BTN e pra como telefone_confirmacao_datahora é preenchida.
_TELEFONE_CONFIRMACAO_VALIDAS = ["PERTENCE", "INDEFINIDO", "NAO_PERTENCE"]


def _normaliza_rotulo(texto) -> str:
    """Baixa a caixa, remove acentos, trata '_' como espaço e colapsa espaços — mesmo
    padrão de _normaliza em tasks/extract.py (mais o tratamento de '_'), usado aqui pra
    comparar rótulo da LLM com o catálogo oficial sem falso negativo por acentuação/caixa/
    separador (cobre tanto natureza/secretaria — "Dúvida", "RioLuz" — quanto relacao_hsm —
    "DENTRO_DO_ESCOPO" — no mesmo código)."""
    texto = str(texto).strip().lower().replace("_", " ")
    texto = unicodedata.normalize("NFKD", texto).encode("ascii", "ignore").decode("ascii")
    return re.sub(r"\s+", " ", texto)


def _valida_rotulo_unico(valor, rotulos_validos: list[str]):
    """Corrige a grafia de volta pra canônica se valor bater (sem acento/caixa) com algum
    de rotulos_validos; retorna None se não bater com nenhum (rótulo fora do catálogo) ou
    se valor já vier vazio."""
    if valor is None or (isinstance(valor, float) and pd.isna(valor)) or not str(valor).strip():
        return None
    lookup = {_normaliza_rotulo(r): r for r in rotulos_validos}
    return lookup.get(_normaliza_rotulo(valor))


def _valida_multi_rotulo(valor, rotulos_validos: list[str]):
    """Mesma ideia de _valida_rotulo_unico, mas pra campo multi-label separado por
    vírgula (ex.: "Dúvida, Problema"): valida e corrige a grafia de cada rótulo
    individualmente, descarta só os que não batem com o catálogo (mantém os outros),
    remove duplicata. Sem nenhum rótulo válido sobrando, retorna None."""
    if valor is None or (isinstance(valor, float) and pd.isna(valor)) or not str(valor).strip():
        return None
    lookup = {_normaliza_rotulo(r): r for r in rotulos_validos}
    validos = []
    for bruto in str(valor).split(","):
        canonico = lookup.get(_normaliza_rotulo(bruto))
        if canonico is not None and canonico not in validos:
            validos.append(canonico)
    return ", ".join(validos) if validos else None


def _valida_principal_e_relacionados(principal_bruto, relacionados_bruto, rotulos_validos: list[str]):
    """Valida o par principal (rótulo único) + relacionados (multi-label) e garante o
    contrato de que relacionados sempre inclui o principal — o prompt pede isso, mas a
    LLM às vezes omite. Principal válido que faltar em relacionados entra na frente.
    Retorna (principal, relacionados)."""
    principal = _valida_rotulo_unico(principal_bruto, rotulos_validos)
    relacionados = _valida_multi_rotulo(relacionados_bruto, rotulos_validos)
    if principal is not None:
        lista = relacionados.split(", ") if relacionados else []
        if principal not in lista:
            relacionados = ", ".join([principal, *lista])
    return principal, relacionados


# Colunas preenchidas pelos prompts de roteamento (ver _roteia em classifica_sessoes) —
# null em sessão não relevante e nas RESPOSTA_ATRASADA_BTN.
_COLUNAS_ROTEAMENTO = [
    "macrotema_principal", "macrotemas_relacionados", "justificativa_macrotema",
    "resposta_llm_bruta_macrotema", "prompt_versao_macrotema",
    "secretaria_principal", "secretarias_relacionadas", "justificativa_secretaria",
    "resposta_llm_bruta_secretaria", "prompt_versao_secretaria",
]


def _soma_tokens(usages: list[dict], chave: str):
    """Soma um campo de usageMetadata entre as chamadas da sessão; None se nenhuma
    resposta trouxe o campo (em vez de 0, que pareceria medido)."""
    valores = [u[chave] for u in usages if u.get(chave) is not None]
    return sum(valores) if valores else None


@task(log_prints=True)
def classifica_sessoes(
    df_prompts: pd.DataFrame,
    bf_key: str,
    base_url: str,
    model: str,
    max_workers: int,
    max_tentativas: int,
    espera_inicial: int,
    classificacao_sem_hsm: str,
    df_regras_tema: pd.DataFrame,
    df_regras_causa: pd.DataFrame,
    classificacao_resposta_atrasada: str,
    justificativa_resposta_atrasada: str,
    prompt_versao: str,
    prompt_versao_macrotema: str,
    prompt_versao_secretaria: str,
    project_id: str,
    dataset_id: str,
    table_id: str,
    tmp_table_id: str,
    tamanho_lote: int,
) -> tuple[int, int, int]:
    """Chama a LLM em paralelo (ThreadPoolExecutor, mesmo padrão do notebook) para cada
    prompt pendente. Sessão que falhar (erro de API, resposta fora do schema esperado)
    NÃO é escrita no resultado — fica ausente e será re-tentada automaticamente na
    próxima execução (dentro da janela de LOOKBACK_DAYS), sem precisar de lógica de
    reprocessamento/DLQ separada.

    Até 3 chamadas por sessão, em sequência dentro do mesmo worker: o prompt principal
    (com_hsm/sem_hsm) e, só se ele devolver conteudo_relevante=true, os prompts de
    macrotema e de secretaria (sessão não relevante não tem assunto pra rotear — e é
    a maioria, então isso corta boa parte das chamadas extras). Tudo ou nada: falha em
    qualquer uma das 3 descarta a sessão inteira (vai pro retry do próximo run, como
    acima) — nunca grava linha com o roteamento pela metade.

    Carrega no BigQuery em lotes de `tamanho_lote` sessões, à medida que vão sendo
    classificadas (monta_dataframe_final + aplica_regras_tema + aplica_regras_causa +
    carrega_classificacoes, por lote), em vez de esperar as ~6mil sessões todas
    terminarem pra escrever de uma vez. Um run desse tamanho pode levar 20-30min de
    chamadas pagas à LLM — sem isso, um crash no meio (pod reiniciado, OOM, cancelamento
    manual) perde TUDO que já foi classificado, porque nada tinha sido persistido ainda
    (nem a tmp: load_table_from_dataframe só roda no final). Com carga incremental, o
    prejuízo de um crash fica limitado a no máximo 1 lote incompleto, não o run inteiro.

    Retorna (n_classificadas_com_sucesso, n_falhas, linhas_carregadas)."""
    if df_prompts.empty:
        return 0, 0, 0

    bifrost = BifrostClient(api_key=bf_key, base=base_url, model=model)

    def _finaliza_e_carrega(buffer: list[dict]) -> int:
        """Formata um lote de resultados da LLM já prontos pro formato da tabela destino
        e carrega no BigQuery. Chamada de dentro do loop abaixo, não só no final."""
        df_chunk = pd.DataFrame(buffer)
        df_chunk = df_prompts[_COLUNAS_METADADO_SESSAO].merge(df_chunk, on="id_sessao", how="inner")
        df_chunk_final = monta_dataframe_final(
            df_classificadas=df_chunk,
            df_pre_classificadas=pd.DataFrame(columns=_COLUNAS_METADADO_SESSAO),
            classificacao_resposta_atrasada=classificacao_resposta_atrasada,
            justificativa_resposta_atrasada=justificativa_resposta_atrasada,
            prompt_versao=prompt_versao,
        )
        df_chunk_final = aplica_regras_tema(df_final=df_chunk_final, df_regras=df_regras_tema)
        df_chunk_final = aplica_regras_causa(df_final=df_chunk_final, df_regras=df_regras_causa)
        return carrega_classificacoes(
            df_final=df_chunk_final,
            project_id=project_id,
            dataset_id=dataset_id,
            table_id=table_id,
            tmp_table_id=tmp_table_id,
        )

    def _pergunta(prompt):
        """Uma chamada à LLM + parse do JSON. Retorna (parsed, texto_bruto, usage)."""
        response = bifrost.ask(
            prompt,
            system_instruction="Responda em PT-BR.",
            temperature=0,
            max_tentativas=max_tentativas,
            espera_inicial=espera_inicial,
        )
        texto_bruto = BifrostClient.extract_text(response)
        # usageMetadata vem de graça na mesma resposta — sem chamada extra à API
        return _parse_json_response(texto_bruto), texto_bruto, response.get("usageMetadata", {})

    def _roteia(hsm_texto, conversa_completa) -> tuple[dict, list[dict]]:
        """Prompts de macrotema e secretaria — mesma entrada (HSM + conversa) nos dois.
        Retorna (campos pra linha destino, usages das chamadas)."""
        hsm = hsm_texto if isinstance(hsm_texto, str) and hsm_texto.strip() else _HSM_AUSENTE
        parsed_mt, bruto_mt, usage_mt = _pergunta(
            _render(_TEMPLATE_MACROTEMA, hsm_texto=hsm, conversa_completa=conversa_completa)
        )
        macrotema_principal, macrotemas_relacionados = _valida_principal_e_relacionados(
            parsed_mt.get("macrotema_principal"), parsed_mt.get("macrotemas_relacionados"), _MACROTEMAS_VALIDOS
        )
        # secretaria depende do macrotema: só as candidatas dele, com "OUTRA" refazendo
        # com a lista completa (ver _template_secretaria)
        candidatas = _secretarias_candidatas(macrotemas_relacionados)
        parsed_sec, bruto_sec, usage_sec = _pergunta(
            _render(_template_secretaria(candidatas), hsm_texto=hsm, conversa_completa=conversa_completa)
        )
        usages = [usage_mt, usage_sec]
        if candidatas is not None and _normaliza_rotulo(
            parsed_sec.get("secretaria_principal") or ""
        ) == _normaliza_rotulo(_SECRETARIA_FORA_DA_LISTA):
            parsed_sec, bruto_completa, usage_completa = _pergunta(
                _render(_template_secretaria(None), hsm_texto=hsm, conversa_completa=conversa_completa)
            )
            bruto_sec = f"{bruto_sec}\n--- lista completa ---\n{bruto_completa}"
            usages.append(usage_completa)
        secretaria_principal, secretarias_relacionadas = _valida_principal_e_relacionados(
            parsed_sec.get("secretaria_principal"), parsed_sec.get("secretarias_relacionadas"), _SECRETARIAS_VALIDAS
        )
        campos = {
            "macrotema_principal": macrotema_principal,
            "macrotemas_relacionados": macrotemas_relacionados,
            "justificativa_macrotema": parsed_mt.get("justificativa_macrotema"),
            "resposta_llm_bruta_macrotema": bruto_mt,
            "prompt_versao_macrotema": prompt_versao_macrotema,
            "secretaria_principal": secretaria_principal,
            "secretarias_relacionadas": secretarias_relacionadas,
            "justificativa_secretaria": parsed_sec.get("justificativa_secretaria"),
            "resposta_llm_bruta_secretaria": bruto_sec,
            "prompt_versao_secretaria": prompt_versao_secretaria,
        }
        return campos, usages

    def _processa(id_sessao, tipo_prompt, prompt, hsm_texto, conversa_completa):
        try:
            parsed, texto_bruto, usage = _pergunta(prompt)
            # o prompt sem_hsm não pergunta "relacao_hsm" pra LLM (não há HSM pra
            # comparar escopo) — mas a coluna cobre toda sessão, então forçamos o
            # rótulo explícito aqui em vez de deixar null (null viraria "não
            # classificado ainda", que é outra coisa). Só o valor vindo da LLM
            # (com_hsm) passa por validação — classificacao_sem_hsm já é uma
            # constante conhecida-válida, não precisa.
            relacao_hsm = (
                classificacao_sem_hsm
                if tipo_prompt == "sem_hsm"
                else _valida_rotulo_unico(parsed.get("relacao_hsm"), _RELACAO_HSM_VALIDAS)
            )
            # telefone_confirmacao_flag (item 7 do prompt) só existe no com_hsm — sem HSM
            # não há "assunto guiado pela HSM" pra avaliar se o cidadão confirmou o
            # número (mesmo raciocínio que exclui SEM_HSM_ASSOCIADO no reprocessamento
            # retroativo, ver quick/telefone_confirmacao_retroativo). Fica null no sem_hsm,
            # não um valor tipo "não avaliado" — null já significa isso em toda a tabela.
            telefone_confirmacao_flag = (
                _valida_rotulo_unico(parsed.get("telefone_confirmacao_flag"), _TELEFONE_CONFIRMACAO_VALIDAS)
                if tipo_prompt == "com_hsm"
                else None
            )
            # Roteamento (macrotema + secretaria) só pra sessão relevante — `is True`
            # e não truthiness: só o booleano JSON true conta (a coluna é BOOL). Fora
            # disso as colunas de roteamento ficam null, incluindo as versões: null
            # aqui = "não avaliado", igual ao resto da tabela.
            usages = [usage]
            roteamento = dict.fromkeys(_COLUNAS_ROTEAMENTO)
            if parsed.get("conteudo_relevante") is True:
                roteamento, usages_roteamento = _roteia(hsm_texto, conversa_completa)
                usages += usages_roteamento
            # prompt_enviado/resposta_llm_bruta continuam sendo só do prompt principal
            # (os de roteamento têm coluna de resposta própria); tokens_* somam as 3.
            return {
                "id_sessao": id_sessao,
                "tipo_prompt": tipo_prompt,
                "relacao_hsm": relacao_hsm,
                "telefone_confirmacao_flag": telefone_confirmacao_flag,
                "conteudo_relevante": parsed.get("conteudo_relevante"),
                "resumo": parsed.get("resumo"),
                "natureza": _valida_multi_rotulo(parsed.get("natureza"), _NATUREZA_VALIDAS),
                "motivo": parsed.get("motivo"),
                "justificativa": parsed.get("justificativa"),
                "resposta_llm_bruta": texto_bruto,
                "modelo": model,
                "prompt_enviado": prompt,
                "tokens_entrada": _soma_tokens(usages, "promptTokenCount"),
                "tokens_saida": _soma_tokens(usages, "candidatesTokenCount"),
                "tokens_total": _soma_tokens(usages, "totalTokenCount"),
                # parte da entrada que veio do cache implícito do Gemini (cobrada com desconto)
                "tokens_cache": _soma_tokens(usages, "cachedContentTokenCount"),
                **roteamento,
                "erro": None,
            }
        except Exception as e:  # qualquer falha aqui vira "não classificado ainda", ver docstring
            return {"id_sessao": id_sessao, "erro": f"{type(e).__name__}: {e}"}

    buffer: list[dict] = []
    n_sucesso = 0
    n_erro = 0
    linhas_carregadas = 0
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(
                _processa, row.id_sessao, row.tipo_prompt, row.prompt, row.hsm_texto, row.conversa_completa
            ): row.id_sessao
            for row in df_prompts.itertuples()
        }
        total = len(futures)
        for i, future in enumerate(as_completed(futures), start=1):
            resultado = future.result()
            if resultado.get("erro"):
                n_erro += 1
                print(f"[CLASSIFY] FALHA id_sessao={resultado['id_sessao']}: {resultado['erro']}")
            else:
                resultado.pop("erro", None)
                buffer.append(resultado)
                n_sucesso += 1

            # flush por tamanho de lote OU no último resultado (garante que sobra de
            # buffer menor que tamanho_lote também é carregada, não só descartada)
            if len(buffer) >= tamanho_lote or (i == total and buffer):
                linhas_carregadas += _finaliza_e_carrega(buffer)
                print(f"[CLASSIFY] lote de {len(buffer)} carregado no BigQuery ({linhas_carregadas} linha(s) no total até agora).")
                buffer = []

            if i % 50 == 0 or i == total:
                print(f"[CLASSIFY] {i}/{total} processados ({n_erro} falha(s) até agora)")

    print(
        f"[CLASSIFY] Concluído: {n_sucesso} classificada(s) com sucesso, "
        f"{n_erro} falha(s) (retry automático no próximo run), {linhas_carregadas} linha(s) carregada(s)."
    )
    return n_sucesso, n_erro, linhas_carregadas


@task(log_prints=True)
def monta_dataframe_final(
    df_classificadas: pd.DataFrame,
    df_pre_classificadas: pd.DataFrame,
    classificacao_resposta_atrasada: str,
    justificativa_resposta_atrasada: str,
    prompt_versao: str,
) -> pd.DataFrame:
    """Junta as classificadas por LLM com as pré-classificadas por regra num único
    DataFrame no formato da tabela destino."""
    agora = datetime.now(tz=timezone.utc)

    if not df_pre_classificadas.empty:
        df_pre_classificadas = df_pre_classificadas.copy()
        df_pre_classificadas["relacao_hsm"] = classificacao_resposta_atrasada
        df_pre_classificadas["justificativa"] = justificativa_resposta_atrasada
        # RESPOSTA_ATRASADA_BTN: clicar num botão do HSM fora da janela de 24h já é
        # engajamento com o assunto — mesma regra do reprocessamento retroativo (ver
        # quick/telefone_confirmacao_retroativo/reprocessa_telefone_confirmacao.py),
        # sem gastar chamada de LLM pra perguntar de novo.
        df_pre_classificadas["telefone_confirmacao_flag"] = "PERTENCE"
        df_pre_classificadas["conteudo_relevante"] = None
        df_pre_classificadas["resumo"] = None
        for coluna in _COLUNAS_ROTEAMENTO:
            df_pre_classificadas[coluna] = None
        df_pre_classificadas["natureza"] = None
        df_pre_classificadas["motivo"] = None
        df_pre_classificadas["resposta_llm_bruta"] = None
        # null, não um valor tipo "regra:...": sessão decidida por regra não passou
        # por modelo nenhum. Essa informação já está inteira em `relacao_hsm`
        # (RESPOSTA_ATRASADA_BTN) — não precisa duplicar aqui.
        df_pre_classificadas["modelo"] = None
        # sem prompt e sem tokens: sessão decidida por regra nunca chama a LLM
        df_pre_classificadas["prompt_enviado"] = None
        df_pre_classificadas["tokens_entrada"] = None
        df_pre_classificadas["tokens_saida"] = None
        df_pre_classificadas["tokens_total"] = None
        df_pre_classificadas["tokens_cache"] = None
        # null, não "resposta_atrasada_btn": tipo_prompt identifica qual PROMPT gerou a
        # classificação (com_hsm/sem_hsm) — resposta atrasada não passou por prompt
        # nenhum, foi decidida por regra.
        df_pre_classificadas["tipo_prompt"] = None

    df_final = pd.concat([df_classificadas, df_pre_classificadas], ignore_index=True)
    if df_final.empty:
        return df_final

    df_final["classificado_em"] = agora
    df_final["data_particao"] = agora.date()
    df_final["prompt_versao"] = prompt_versao

    # telefone_confirmacao_datahora: datahora da sessão (sessao_fim_datahora) que
    # originou telefone_confirmacao_flag — não a datahora em que a classificação
    # rodou (essa já é classificado_em). Fim, não início: fim_datahora preenchido é
    # o próprio critério que torna a sessão "pronta pra classificar" na extração
    # (ver SOURCE_TABLE em constants.py) — garantido não-nulo pra toda sessão que
    # chega aqui, o que início não tem documentado. Fica null junto com a flag
    # (sem_hsm), nunca sozinha. sessao_fim_datahora já está em df_final nos 2 casos
    # (LLM e pré-classificada) — ver _COLUNAS_METADADO_SESSAO.
    df_final["telefone_confirmacao_datahora"] = df_final["sessao_fim_datahora"].where(
        df_final["telefone_confirmacao_flag"].notna()
    )

    # Etapas 2/3 (tema, causa sistêmica) — reservadas, array vazio: esta pipeline só
    # faz a etapa 1. Quando a pipeline de tema/motivo existir, ela faz MERGE nesta
    # mesma linha (por id_sessao) e preenche de verdade — "já passou pela etapa" a
    # partir daí é só ARRAY_LENGTH(coluna) > 0, sem precisar de flag own separada.
    df_final["tema_nome"] = [[] for _ in range(len(df_final))]
    df_final["causa_nome"] = [[] for _ in range(len(df_final))]

    # sentimento (positivo/negativo/neutro) — derivado por regra a partir de
    # natureza, não pela LLM (ver docstring de _deriva_sentimento)
    df_final["sentimento"] = df_final["natureza"].apply(_deriva_sentimento)

    if "conteudo_relevante" in df_final.columns:
        df_final["conteudo_relevante"] = df_final["conteudo_relevante"].astype("boolean")

    print(f"[CLASSIFY] {len(df_final)} linha(s) prontas pra carregar no BigQuery.")
    return df_final
