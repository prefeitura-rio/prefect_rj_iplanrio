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

import json
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests
from prefect import task

from pipelines.rj_crm__agentforce_classificacao_llm.tasks.load import carrega_classificacoes
from pipelines.rj_crm__agentforce_classificacao_llm.tasks.taxonomia import aplica_regras_causa, aplica_regras_tema

_PROMPTS_DIR = Path(__file__).resolve().parent.parent / "prompts"

# Colunas de metadado da sessão (vindas da extração/enriquecimento) que precisam
# sobreviver até a tabela destino — tanto pras sessões que passam pela LLM quanto
# pras pré-classificadas por regra. _processa (em classifica_sessoes) não as
# recebe: ela só sabe o que a LLM devolveu, então elas voltam via merge por
# id_sessao depois. mensagens_usuario_concatenadas/hsm_texto entram aqui pra
# permitir validar a classificação (resumo/motivo/tema) lendo a conversa original
# direto da tabela destino, sem precisar reconstruir a partir de prompt_enviado
# (que é null nas sessões RESPOSTA_ATRASADA_BTN, decididas por regra sem LLM).
_COLUNAS_METADADO_SESSAO = [
    "id_sessao", "telefone", "cpf", "nome_cidadao", "sessao_inicio_datahora",
    "sessao_fim_datahora", "jornada_nome", "id_jornada", "id_disparo_hsm", "hsm_envio_datahora",
    "mensagens_usuario_concatenadas", "hsm_texto",
]
_TEMPLATE_COM_HSM = (_PROMPTS_DIR / "classificacao_hsm.txt").read_text()
_TEMPLATE_SEM_HSM = (_PROMPTS_DIR / "classificacao_sem_hsm.txt").read_text()


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
            _TEMPLATE_COM_HSM, hsm_texto=row["hsm_texto"], conversa=row["mensagens_usuario_concatenadas"]
        ),
        axis=1,
    )

    df_sem_hsm = df_enriquecido.loc[enviar_llm_sem_hsm].copy()
    df_sem_hsm["tipo_prompt"] = "sem_hsm"
    df_sem_hsm["prompt"] = df_sem_hsm["mensagens_usuario_concatenadas"].apply(
        lambda conversa: _render(_TEMPLATE_SEM_HSM, conversa=conversa)
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

    def _processa(id_sessao, tipo_prompt, prompt):
        try:
            response = bifrost.ask(
                prompt,
                system_instruction="Responda em PT-BR.",
                temperature=0,
                max_tentativas=max_tentativas,
                espera_inicial=espera_inicial,
            )
            texto_bruto = BifrostClient.extract_text(response)
            parsed = _parse_json_response(texto_bruto)
            # o prompt sem_hsm não pergunta "classificacao" pra LLM (não há HSM pra
            # comparar escopo) — mas a coluna cobre toda sessão, então forçamos o
            # rótulo explícito aqui em vez de deixar null (null viraria "não
            # classificado ainda", que é outra coisa)
            classificacao = classificacao_sem_hsm if tipo_prompt == "sem_hsm" else parsed.get("classificacao")
            # usageMetadata vem de graça na mesma resposta — sem chamada extra à API
            usage = response.get("usageMetadata", {})
            return {
                "id_sessao": id_sessao,
                "tipo_prompt": tipo_prompt,
                "classificacao": classificacao,
                "conteudo_relevante": parsed.get("conteudo_relevante"),
                "resumo": parsed.get("resumo"),
                "secretaria_relacionada": parsed.get("secretaria_relacionada"),
                "sentimento": parsed.get("sentimento"),
                "motivo": parsed.get("motivo"),
                "justificativa": parsed.get("justificativa"),
                "resposta_llm_bruta": texto_bruto,
                "modelo": model,
                "prompt_enviado": prompt,
                "tokens_entrada": usage.get("promptTokenCount"),
                "tokens_saida": usage.get("candidatesTokenCount"),
                "tokens_total": usage.get("totalTokenCount"),
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
            executor.submit(_processa, row.id_sessao, row.tipo_prompt, row.prompt): row.id_sessao
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
        df_pre_classificadas["classificacao"] = classificacao_resposta_atrasada
        df_pre_classificadas["justificativa"] = justificativa_resposta_atrasada
        df_pre_classificadas["conteudo_relevante"] = None
        df_pre_classificadas["resumo"] = None
        df_pre_classificadas["secretaria_relacionada"] = None
        df_pre_classificadas["sentimento"] = None
        df_pre_classificadas["motivo"] = None
        df_pre_classificadas["resposta_llm_bruta"] = None
        # null, não um valor tipo "regra:...": sessão decidida por regra não passou
        # por modelo nenhum. Essa informação já está inteira em `classificacao`
        # (RESPOSTA_ATRASADA_BTN) — não precisa duplicar aqui.
        df_pre_classificadas["modelo"] = None
        # sem prompt e sem tokens: sessão decidida por regra nunca chama a LLM
        df_pre_classificadas["prompt_enviado"] = None
        df_pre_classificadas["tokens_entrada"] = None
        df_pre_classificadas["tokens_saida"] = None
        df_pre_classificadas["tokens_total"] = None
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

    # Etapas 2/3 (tema, causa sistêmica) — reservadas, array vazio: esta pipeline só
    # faz a etapa 1. Quando a pipeline de tema/motivo existir, ela faz MERGE nesta
    # mesma linha (por id_sessao) e preenche de verdade — "já passou pela etapa" a
    # partir daí é só ARRAY_LENGTH(coluna) > 0, sem precisar de flag own separada.
    df_final["tema_nome"] = [[] for _ in range(len(df_final))]
    df_final["causa_nome"] = [[] for _ in range(len(df_final))]

    if "conteudo_relevante" in df_final.columns:
        df_final["conteudo_relevante"] = df_final["conteudo_relevante"].astype("boolean")

    print(f"[CLASSIFY] {len(df_final)} linha(s) prontas pra carregar no BigQuery.")
    return df_final
