"""Decide se o modelo novo deve ser promovido a champion, e publica a versão no GCS.

3 critérios, todos precisam passar (quando aplicável):

  1. Probabilidade não degenerada — checagem RÁPIDA (desvio padrão do score previsto numa
     amostra do treino), antes de gastar tempo com a simulação inteira. Um modelo
     degenerado (toda linha com a mesma nota) já reprovaria no critério 3 de qualquer
     jeito — isso só dá um erro mais claro e mais rápido (não precisa esperar a simulação
     pra descobrir que o treino quebrou).
  2. Gap da simulação do modelo novo >= gap do modelo em produção, no mesmo pool. Só vale
     quando já existe champion — na 1ª execução (sem nada pra comparar), esse critério é
     pulado.
  3. O modelo novo vence o aleatório com p < 0.05 (teste pareado do gap, ver
     ``avaliar_simulacao.teste_pareado``).

AUC do held-out e "schema bate" NÃO entram no gate (decisão do usuário, 2026-09-23): AUC é
só um proxy — a medida real é o gap da simulação, que já é o critério 2; e "schema bate" já
é uma trava dura em ``utils/modelo_store.py::publica_versao`` (recusa publicar se as
features não baterem) — não precisa ser reavaliada aqui.

Promover é só trocar o ``champion.json`` (ver ``utils/modelo_store.py``) — o modelo em si
já foi treinado com 100% dos dados em ``treinar.py``. Toda versão treinada é publicada no
GCS (aprovada ou não — histórico/depuração), só o ponteiro do champion muda condicionalmente.
"""

from dataclasses import dataclass

import numpy as np
from google.cloud import storage
from iplanrio.pipelines_utils.env import get_bd_credentials_from_env
from prefect import task
from prefect_rj_iplanrio.logging import get_logger

from pipelines.rj_crm__modelo_qualidade_telefone import constants
from pipelines.rj_crm__modelo_qualidade_telefone.constants import FEATURES
from pipelines.rj_crm__modelo_qualidade_telefone.tasks.retreino.avaliar_simulacao import (
    NOME_MODELO_NOVO,
    NOME_MODELO_PRODUCAO,
    ResultadoSimulacao,
)
from pipelines.rj_crm__modelo_qualidade_telefone.tasks.retreino.treinar import ResultadoTreino
from pipelines.rj_crm__modelo_qualidade_telefone.utils import modelo_store

logger = get_logger(__name__)

P_VALOR_MAXIMO = 0.05
# Na escala do score bruto (log-odds, base excluída — ver verifica_probabilidade_nao_degenerada).
# Qualquer modelo funcionando de verdade tem desvio bem acima disso (o champion atual, por
# exemplo, produz probabilidades de p01=0.09 a p99=0.91 — bem longe de degenerado); só um
# modelo quebrado (feature quase constante, rótulo errado etc.) cairia abaixo.
DESVIO_PADRAO_MINIMO_SCORE = 0.01


@dataclass(frozen=True)
class DecisaoGate:
    """Resultado do gate: promove ou não, e por quê — sempre com os valores medidos, mesmo
    quando o critério passou (pro relatório mostrar tudo, não só o motivo da reprovação)."""

    promovido: bool
    motivos: list[str]  # 1 por critério reprovado (vazio se promovido)
    probabilidade_degenerada: bool
    gap_novo: float
    gap_producao: float | None  # None quando não há champion pra comparar
    p_valor_vs_aleatorio: float | None  # None quando o teste não pôde ser calculado


def verifica_probabilidade_nao_degenerada(resultado_treino: ResultadoTreino) -> tuple[bool, str | None]:
    """O modelo não colapsou? Mede o desvio padrão do score bruto (soma dos SHAP, a base é
    constante e não muda o desvio) numa amostra do próprio treino.

    :returns: ``(passou, motivo)`` — ``motivo`` é ``None`` se passou.
    """
    score = resultado_treino.amostra_shap.shap_values.sum(axis=1)
    desvio = float(np.std(score))
    if desvio < DESVIO_PADRAO_MINIMO_SCORE:
        return False, (
            f"probabilidade degenerada: desvio padrão do score = {desvio:.2e} "
            f"(mínimo {DESVIO_PADRAO_MINIMO_SCORE:.0e})"
        )
    return True, None


def avalia_gate(
    resultado_treino: ResultadoTreino, resultado_simulacao: ResultadoSimulacao, exigir_gate: bool = True
) -> DecisaoGate:
    """Aplica os 3 critérios e decide se o modelo novo deve ser promovido.

    :param resultado_treino: Saída de ``treinar.treina``.
    :param resultado_simulacao: Saída de ``avaliar_simulacao.avalia_simulacao`` — chamada
        com o MESMO booster de ``resultado_treino`` como ``booster_novo``.
    :param exigir_gate: Se ``False``, força ``promovido=True`` independente dos critérios
        (os motivos que teriam reprovado continuam registrados, só não bloqueiam).
    """
    motivos: list[str] = []

    prob_ok, motivo_prob = verifica_probabilidade_nao_degenerada(resultado_treino)
    if not prob_ok:
        motivos.append(motivo_prob)

    gap_novo = float(resultado_simulacao.resumo.loc[NOME_MODELO_NOVO, "gap"])
    tem_producao = NOME_MODELO_PRODUCAO in resultado_simulacao.resumo.index
    gap_producao = float(resultado_simulacao.resumo.loc[NOME_MODELO_PRODUCAO, "gap"]) if tem_producao else None
    if gap_producao is not None and gap_novo < gap_producao:
        motivos.append(
            f"gap do modelo novo ({gap_novo:.1f}) menor que o do modelo em produção ({gap_producao:.1f})"
        )

    p_valor = resultado_simulacao.teste_vs_aleatorio.p_valor if resultado_simulacao.teste_vs_aleatorio else None
    if p_valor is None or p_valor >= P_VALOR_MAXIMO:
        sufixo = f" (p={p_valor:.4f})" if p_valor is not None else " (teste não pôde ser calculado)"
        motivos.append("não venceu o aleatório com significância" + sufixo)

    return DecisaoGate(
        promovido=(not motivos) or not exigir_gate,
        motivos=motivos,
        probabilidade_degenerada=not prob_ok,
        gap_novo=gap_novo,
        gap_producao=gap_producao,
        p_valor_vs_aleatorio=p_valor,
    )


def metadata_da_versao(resultado_treino: ResultadoTreino, decisao: DecisaoGate) -> dict:
    """Monta o ``metadata.json`` da versão — features, hiperparâmetros, métricas e a
    decisão do gate (pra auditoria: por que essa versão foi ou não promovida)."""
    return {
        "features": FEATURES,
        "hiperparametros": resultado_treino.hiperparametros,
        "metricas_held_out": resultado_treino.metricas_held_out,
        "n_treino": resultado_treino.n_treino,
        "n_positivos": resultado_treino.n_positivos,
        "taxa_base_high_delivery": resultado_treino.taxa_base_high_delivery,
        "gate": {
            "promovido": decisao.promovido,
            "motivos": decisao.motivos,
            "gap_novo": decisao.gap_novo,
            "gap_producao": decisao.gap_producao,
            "p_valor_vs_aleatorio": decisao.p_valor_vs_aleatorio,
        },
    }


def promove_se_aprovado(
    resultado_treino: ResultadoTreino,
    decisao: DecisaoGate,
    raiz_modelos: str,
    versao: str,
    client: storage.Client | None = None,
    promocao_automatica: bool = True,
) -> None:
    """Publica a versão no GCS (sempre) e promove o ``champion.json`` só se aprovada E
    ``promocao_automatica=True``.

    :param raiz_modelos: Raiz das versões do modelo no GCS (ou diretório local, testes).
    :param versao: Nome da pasta da versão — data e hora do treino, BRT (mesma versão do relatório
        do Drive, ver ``relatorio.py``).
    :param client: Cliente do GCS, obrigatório se ``raiz_modelos`` for ``gs://``.
    :raises FileExistsError: Se ``versao`` já foi publicada antes (versão é imutável).
    """
    metadata = metadata_da_versao(resultado_treino, decisao)
    modelo_store.publica_versao(raiz_modelos, versao, resultado_treino.booster, metadata, client=client)

    if decisao.promovido and promocao_automatica:
        modelo_store.promove_versao(raiz_modelos, versao, client=client)
        logger.info("Versão %s promovida a champion.", versao)
    elif decisao.promovido:
        logger.info("Versão %s aprovada no gate, mas promocao_automatica=False — publicada sem promover.", versao)
    else:
        logger.warning("Versão %s NÃO promovida: %s", versao, "; ".join(decisao.motivos))


@task
def avalia_gate_task(
    resultado_treino: ResultadoTreino, resultado_simulacao: ResultadoSimulacao, exigir_gate: bool = True
) -> DecisaoGate:
    """Task-wrapper fina de :func:`avalia_gate`."""
    return avalia_gate(resultado_treino, resultado_simulacao, exigir_gate)


@task
def promove_se_aprovado_task(
    resultado_treino: ResultadoTreino,
    decisao: DecisaoGate,
    raiz_modelos: str,
    versao: str,
    environment: str,
    promocao_automatica: bool = True,
) -> None:
    """Task-wrapper de :func:`promove_se_aprovado` — autentica no GCS com o secret do work pool."""
    credentials = get_bd_credentials_from_env(mode=environment)
    client = storage.Client(credentials=credentials, project=constants.PROJECT_ID)
    promove_se_aprovado(resultado_treino, decisao, raiz_modelos, versao, client, promocao_automatica)
