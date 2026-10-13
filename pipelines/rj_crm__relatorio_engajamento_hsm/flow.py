# -*- coding: utf-8 -*-
"""
Flow — Relatório de Engajamento por campanha de WhatsApp (HSM).

OBJETIVO
--------
Mede como os cidadãos responderam às campanhas de WhatsApp (HSM/template) da
Prefeitura: classifica por LLM o assunto de cada conversa com resposta, num catálogo de
categorias validado pela secretaria responsável, audita a classificação com uma segunda
LLM ("juiz") e publica o relatório no Google Drive. As revisões que a secretaria faz
nas conversas classificadas viram exemplos (RAG) que melhoram as classificações
seguintes da mesma campanha.

GATILHO — Google Form "Solicitação de Relatório de Engajamento"
----------------------------------------------------------------
As respostas do Form caem no Sheet de solicitações (config.SOLICITACOES_SHEET_ID), que
o flow lê e onde grava o status de cada pedido. Campos do Form: Campanha (lista
sincronizada com nome_campanha_limpo da disparos_ativos — bate com hsm.nome_campanha
na rmi_conversas.chatbot), Tipo de solicitação, Data de início e de fim dos disparos,
Data de geração.

FREQUÊNCIA: 3x por dia, 8h, 13h e 18h (America/Sao_Paulo) — ver prefect.yaml. Cada
execução processa as solicitações com status vazio e Data de geração até hoje: data
futura sai às 8h do dia escolhido; data de hoje sai na próxima execução.

TIPOS DE SOLICITAÇÃO
--------------------
1. Material de validação (Fase 1, primeiro disparo) — tasks/material_validacao.py.
   Descobre as categorias por LLM e publica na pasta da campanha o Sheet `Catálogo`
   (Categoria, Descrição, Exemplos de conversas, Decisão [lista suspensa
   Manter/Excluir/Nova], Justificativa, Data da decisão) e o
   `exemplos_nao_classificado` (todas as conversas do período). Se já existir um
   `Catálogo` (nome exato) na pasta, nada é gerado — renomeie o atual (ex.:
   Catálogo_old) pra gerar de novo.
2. Relatório de engajamento (Fase 2 e recorrente) — tasks/relatorio.py.
   Usa o Catálogo validado (só a coluna Decisão importa: Manter/Nova = ativa, Excluir =
   fora, vazia = ignorada com aviso). Antes de classificar, ingere no banco de exemplos
   as planilhas `conversas_classificadas_revisada_AAAA-MM-DD` da campanha. Classifica
   cada conversa (1 chamada de LLM por conversa; few-shot com as 3 revisões mais
   parecidas da MESMA campanha, ou zero-shot se ainda não há), roda o juiz e publica.
   Também roda a descoberta partindo do Catálogo e lista no fim do relatório as
   categorias NOVAS sugeridas — só sugestão: não entram no Catálogo nem na
   classificação (pra usar, a secretaria inclui no Catálogo com Decisão "Nova").
3. Relatório rápido (catálogo automático) — mesma task, sem Catálogo validado nem RAG:
   descobre as categorias do zero e classifica com elas. Não cria/altera o Catálogo e
   nunca alimenta o banco de exemplos.

RESULTADOS NO DRIVE (pasta raiz: drive_pasta_raiz_id)
-------------------------------------------------------
  <campanha>/Catálogo, <campanha>/exemplos_nao_classificado        (material de validação)
  <campanha>/<data>_<hora>h__disparos_<inicio>_a_<fim>[__catalogo_automatico]/
      relatorio_engajamento (Google Doc), conversas_classificadas (Google Sheet;
      no relatório de engajamento com as colunas revisao [Certo/Errado],
      categorias_corretas, justificativa_revisao), catalogo_automatico (só no rápido)
Cada geração cria uma subpasta nova — nunca substitui uma anterior (que pode ter uma
planilha revisada). CPF e telefone não vão pra nenhum arquivo.

STATUS (coluna `status` do Sheet de solicitações; `link_resultado` aponta o resultado)
--------------------------------------------------------------------------------------
  vazio                    aguardando a data de geração
  processando              em execução (preso há mais de 6h -> vira erro na próxima)
  concluido                gerado (pode vir com " — N categoria(s) sem decisão...")
  pulado: <motivo>         não gerou de propósito (Catálogo já existe, sem conversas...)
  aguardando liberação: N  mais conversas que teto_conversas — ver LIBERAR ABAIXO
  erro: <motivo>           falhou; corrigir a causa e enviar o Form de novo

LIBERAR UMA CAMPANHA ACIMA DO TETO
----------------------------------
Rodar o deployment manualmente (Custom run) com campanhas_liberadas=["<campanha>"].
Só as solicitações dessa campanha que JÁ estavam "aguardando liberação" passam do teto;
pedidos novos dela seguem a regra normal. Vale só pra essa execução.

FALHA NO PREFECT
----------------
Cada solicitação é processada isolada: erro numa não impede as outras. Erro esperado
(Catálogo ausente/não validado) só vai pro status. Erro inesperado (LLM/Drive/BQ fora
do ar, bug) vai pro status com o id do run E faz o flow terminar como Failed, depois de
processar todas — pra disparar o alerta de falha.
"""

from __future__ import annotations

import os
import traceback
from collections import Counter
from datetime import date

from iplanrio.pipelines_utils.env import inject_bd_credentials_task
from iplanrio.pipelines_utils.logging import log
from iplanrio.pipelines_utils.prefect import rename_current_flow_run_task
from prefect import flow
from prefect.runtime import flow_run

from pipelines.rj_crm__relatorio_engajamento_hsm.config import (
    BIFROST_MODEL,
    CONCORRENCIA_LLM,
    DRIVE_PASTA_RAIZ_ID,
    N_EXEMPLOS_POR_CATEGORIA,
    N_EXEMPLOS_RAG,
    PISO_JUIZ_POR_CATEGORIA,
    TAMANHO_LOTE_DESCOBERTA,
    TAMANHO_LOTE_JUIZ,
    TETO_AMOSTRA_DESCOBERTA,
    TETO_CONVERSAS,
    TETO_JUIZ,
    TIPO_MATERIAL_VALIDACAO,
)
from pipelines.rj_crm__relatorio_engajamento_hsm.tasks.material_validacao import gera_material_validacao
from pipelines.rj_crm__relatorio_engajamento_hsm.tasks.relatorio import ParametrosRelatorio, gera_relatorio
from pipelines.rj_crm__relatorio_engajamento_hsm.tasks.solicitacoes import (
    ResultadoSolicitacao,
    Solicitacao,
    SolicitacaoError,
    agora_brt,
    carrega_solicitacoes,
    registra_status,
    status_erro,
)
from pipelines.rj_crm__relatorio_engajamento_hsm.utils.bifrost import BifrostClient


def _processa(solicitacao: Solicitacao, bifrost: BifrostClient, params: ParametrosRelatorio) -> ResultadoSolicitacao:
    if solicitacao.tipo == TIPO_MATERIAL_VALIDACAO:
        return gera_material_validacao(
            solicitacao,
            params.drive_pasta_raiz_id,
            bifrost,
            params.tamanho_lote_descoberta,
            params.teto_amostra_descoberta,
        )
    return gera_relatorio(solicitacao, bifrost, params)


def _categoria_status(status: str) -> str:
    """'pulado: X' -> 'pulado'; 'concluido — obs' -> 'concluido' (só pro resumo do log)."""
    return status.split(":")[0].split(" —")[0].strip()


@flow(log_prints=True)
def rj_crm__relatorio_engajamento_hsm(  # noqa: PLR0913 — parâmetros de flow, documentados abaixo
    campanhas_liberadas: list[str] | None = None,
    data_referencia: str | None = None,
    teto_conversas: int = TETO_CONVERSAS,
    teto_juiz: int = TETO_JUIZ,
    piso_juiz_por_categoria: int = PISO_JUIZ_POR_CATEGORIA,
    teto_amostra_descoberta: int = TETO_AMOSTRA_DESCOBERTA,
    concorrencia_llm: int = CONCORRENCIA_LLM,
    n_exemplos_rag: int = N_EXEMPLOS_RAG,
    tamanho_lote_juiz: int = TAMANHO_LOTE_JUIZ,
    tamanho_lote_descoberta: int = TAMANHO_LOTE_DESCOBERTA,
    n_exemplos_por_categoria: int = N_EXEMPLOS_POR_CATEGORIA,
    sugerir_categorias: bool = True,
    bifrost_model: str = BIFROST_MODEL,
    drive_pasta_raiz_id: str = DRIVE_PASTA_RAIZ_ID,
) -> None:
    """Processa as solicitações pendentes do Form de relatório de engajamento.

    Args:
        campanhas_liberadas: Campanhas (nome exato do Form) cujas solicitações em
            "aguardando liberação" podem passar do teto_conversas NESTA execução. Ex.:
            ["smas_cadunico_desatualizado"]. Pedidos novos dessas campanhas não são
            liberados. Vazio (padrão) = ninguém passa do teto.
        data_referencia: "Hoje" no formato AAAA-MM-DD, pra decidir quais solicitações
            já chegaram na Data de geração. Vazio (padrão) = hoje em America/Sao_Paulo.
            Use só pra testar/reprocessar.
        teto_conversas: LIMITE DE LLM — máximo de conversas com resposta num relatório
            (de engajamento ou rápido). Acima disso o relatório não é gerado: a
            solicitação fica "aguardando liberação" (ver campanhas_liberadas). Mudar
            aqui vale pra TODAS as campanhas da execução.
        teto_juiz: LIMITE DE LLM — máximo de conversas avaliadas pelo juiz. Até esse
            número ele avalia todas; acima (só em campanha liberada), uma amostra
            estratificada por categoria, e o relatório mostra margem de erro.
        piso_juiz_por_categoria: Na amostra do juiz (acima de teto_juiz), quantas
            conversas cada categoria recebe antes da divisão proporcional — garante
            precisão medida até nas categorias pequenas.
        teto_amostra_descoberta: LIMITE DE LLM — máximo de conversas lidas na descoberta
            de categorias (material de validação e relatório rápido). Acima disso a
            descoberta usa uma amostra aleatória desse tamanho.
        concorrencia_llm: Quantas chamadas de LLM da classificação (1 por conversa)
            rodam em paralelo. Mais rápido com mais, mas sujeito ao rate limit do
            Bifrost.
        n_exemplos_rag: Quantos exemplos revisados (conversas parecidas da mesma
            campanha) vão no prompt de cada conversa no relatório de engajamento.
            0 = zero-shot sempre.
        tamanho_lote_juiz: Conversas por chamada de LLM do juiz.
        tamanho_lote_descoberta: Conversas por chamada de LLM da descoberta de
            categorias.
        n_exemplos_por_categoria: Quantos exemplos de conversa aparecem por categoria
            no documento do relatório (não afeta custo de LLM).
        sugerir_categorias: No relatório de engajamento, roda também a descoberta de
            categorias partindo do Catálogo e lista no fim do relatório os temas novos
            que surgiram (não entram no Catálogo nem na classificação). Custa até
            ~teto_amostra_descoberta/tamanho_lote_descoberta chamadas de LLM a mais.
            Desligar = relatório sem a seção de sugestões.
        bifrost_model: Modelo (via gateway Bifrost) usado em todas as etapas de LLM.
        drive_pasta_raiz_id: ID da pasta raiz no Drive ("Relatórios de Engajamento").
            Precisa estar compartilhada (Editor) com a service account de
            BASEDOSDADOS_CREDENTIALS_PROD. Staging pode apontar pra uma pasta de teste.
    """
    rename_current_flow_run_task(new_name="relatorio_engajamento")
    inject_bd_credentials_task(environment="prod")

    bf_key = os.environ.get("BF_KEY")
    if not bf_key:
        raise ValueError(
            "BF_KEY não encontrada nas variáveis de ambiente — adicionar ao secret do work "
            "pool (mesmo secretName usado por rj_crm__agentforce_classificacao_llm)."
        )

    hoje = date.fromisoformat(data_referencia) if data_referencia else agora_brt().date()
    run_id = flow_run.id
    solicitacoes = carrega_solicitacoes(hoje, run_id, campanhas_liberadas or [])
    if not solicitacoes:
        log(f"[FLOW] Nenhuma solicitação a processar ({hoje}).")
        return

    bifrost = BifrostClient(api_key=bf_key, model=bifrost_model)
    params = ParametrosRelatorio(
        drive_pasta_raiz_id=drive_pasta_raiz_id,
        teto_conversas=teto_conversas,
        concorrencia_llm=concorrencia_llm,
        n_exemplos_rag=n_exemplos_rag,
        tamanho_lote_juiz=tamanho_lote_juiz,
        teto_juiz=teto_juiz,
        piso_juiz_por_categoria=piso_juiz_por_categoria,
        tamanho_lote_descoberta=tamanho_lote_descoberta,
        teto_amostra_descoberta=teto_amostra_descoberta,
        n_exemplos_por_categoria=n_exemplos_por_categoria,
        sugerir_categorias=sugerir_categorias,
    )

    resumo: Counter = Counter()
    inesperados: list[str] = []
    for solicitacao in solicitacoes:
        log(f"[FLOW] {solicitacao.descricao()} iniciando...")
        try:
            resultado = _processa(solicitacao, bifrost, params)
        except SolicitacaoError as exc:
            resultado = ResultadoSolicitacao(status_erro(str(exc)))
        except Exception as exc:  # isola a solicitação; o flow falha no fim (ver docstring do módulo)
            log(f"[FLOW] {solicitacao.descricao()} erro inesperado:\n{traceback.format_exc()}")
            inesperados.append(f"{solicitacao.descricao()} {type(exc).__name__}: {exc}")
            resultado = ResultadoSolicitacao(status_erro(f"{type(exc).__name__}: {exc}", run_id))
        registra_status(solicitacao, resultado.status, resultado.link)
        resumo[_categoria_status(resultado.status)] += 1

    log(f"[FLOW] Concluído: {dict(resumo)}.")
    if inesperados:
        raise RuntimeError(
            f"{len(inesperados)} solicitação(ões) com erro inesperado (status já gravado no Sheet):\n"
            + "\n".join(inesperados)
        )
