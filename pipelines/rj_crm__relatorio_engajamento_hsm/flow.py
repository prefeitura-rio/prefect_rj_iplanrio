# -*- coding: utf-8 -*-
"""
Flow diário — Relatório de Engajamento por HSM.

OBJETIVO
--------
Mede como os cidadãos responderam a um disparo de WhatsApp (HSM/template) da
Prefeitura: classifica por LLM o assunto de cada resposta, audita essa
classificação com uma segunda LLM ("juiz") e publica um relatório com volume e
precisão por categoria de assunto — pra quem enviou o HSM entender do que os
cidadãos trataram e o quanto confiar nessa classificação automática.

FREQUÊNCIA
----------
1x por dia, às 8h (America/Sao_Paulo) — ver schedule de produção em prefect.yaml.

GATILHO — o que o flow observa na tabela de disparos
-----------------------------------------------------
Tabela: `rj-crm-registry.brutos_salesforce_staging.disparos_ativos` (planilha
Google Sheets importada no BigQuery, preenchida manualmente pelo time de CRM).
A cada execução, processa toda linha que tiver as 3 colunas abaixo preenchidas:
  - nome_campanha: identifica o HSM (mesmo valor de hsm.nome_hsm em
    rmi_conversas.chatbot — é o que o flow usa pra filtrar as conversas).
  - relatorio_engajamento_data_geracao: dispara o processamento quando essa
    data bate com a data de referência da execução (por padrão, hoje).
  - relatorio_engajamento_data_disparo: data a partir da qual o flow busca as
    conversas desse HSM.

RESULTADOS E ONDE FICAM
------------------------
Publicados no Google Drive, pasta "Relatórios de Engajamento":
https://drive.google.com/drive/folders/1wpZzSMv6dQ5Y50qWIooqRikpkVCh5u2f
— numa subpasta por HSM. Cada rodada (1 HSM + 1 data de geração) gera 3
arquivos, e é idempotente: rodar de novo no mesmo dia pro mesmo HSM não duplica
nada (só uma nova data de geração produz uma rodada nova).
  - relatorio_engajamento_<hsm>__geracao_<data>.docx — o relatório em si.
  - categorias_<...>.csv — 1 linha por categoria de assunto encontrada.
  - classificacoes_<...>.csv — 1 linha por conversa classificada.

COMO OS RESULTADOS SÃO PRODUZIDOS
-----------------------------------
1. Extração (tasks/extract.py): busca em rmi_conversas.chatbot todas as
   sessões que receberam o HSM desde a data de disparo e reconstrói cada uma
   como 1 conversa (falas do CIDADÃO e do AGENTE de IA, em ordem cronológica)
   — só entram sessões com pelo menos 1 resposta do cidadão.
2. Descoberta de categoria — LLM, passada 1 (tasks/discovery.py): olha uma
   amostra ALEATÓRIA das conversas e descobre quais assuntos aparecem,
   montando um catálogo (nome + descrição por categoria). Só esta etapa pode
   criar categoria nova — existe pra evitar viés de ordem (ninguém fica preso
   a um catálogo incompleto de quem foi classificado primeiro).
3. Classificação — LLM, passada 2 (tasks/classify.py): classifica CADA
   conversa numa categoria JÁ EXISTENTE no catálogo (nunca cria uma nova),
   com resumo de 1 frase e justificativa.
4. Julgamento / LLM como juiz (tasks/judge.py): uma segunda chamada de LLM
   audita uma amostra da classificação (100% se a categoria é pequena, uma
   fração se é grande — ver parâmetro limiar_julga_tudo) e diz se concorda
   (CORRETO/INCORRETO). Isso alimenta a precisão por categoria (com margem de
   erro, intervalo de confiança de 95%) mostrada no relatório.
5. Geração e publicação (tasks/report.py): monta o .docx e os 2 CSVs com o
   resultado acima e sobe tudo pro Drive.

O QUE O RELATÓRIO (.docx) MOSTRA
----------------------------------
  - Cabeçalho: nome do HSM, campanha, eixo, data de geração.
  - Resumo: total de disparos, quantos tiveram resposta do cidadão (taxa de
    engajamento), quantos foram classificados, quantos foram avaliados pelo
    juiz, e a precisão média da classificação (com margem de erro).
  - Texto do HSM enviado (contexto do disparo).
  - Observações importantes: é uma análise pontual do disparo (não é
    monitoramento contínuo); é gerado por LLM e pode conter erro; os 2 CSVs
    anexos na mesma pasta usam ";" (ponto e vírgula) como separador de coluna.
  - A query SQL usada pra extrair as conversas (pra reprodutibilidade).
  - Tabela com todas as categorias: nome, quantidade, % do engajamento,
    precisão (do juiz).
  - Por categoria: descrição, métricas, e até N exemplos reais de conversa
    (com o resumo gerado pela LLM) — N é o parâmetro n_exemplos_por_categoria.

COLUNAS DO CSV categorias_<...>.csv (1 linha por categoria)
--------------------------------------------------------------
  - categoria: nome da categoria.
  - descricao: descrição da categoria (definida na descoberta).
  - qtd: quantas conversas caíram nessa categoria.
  - pct_do_engajamento: % que a categoria representa do total de conversas
    classificadas nesse disparo.
  - n_avaliadas_juiz: quantas conversas dessa categoria foram auditadas pelo
    juiz LLM.
  - precisao: fração de veredito "CORRETO" entre as avaliadas pelo juiz
    (0 a 1; vazio se nenhuma foi avaliada).
  - margem_erro_wilson_95: margem de erro (intervalo de confiança de 95%,
    Wilson com correção de população finita) dessa precisão.

COLUNAS DO CSV classificacoes_<...>.csv (1 linha por conversa classificada)
-------------------------------------------------------------------------------
  - id_sessao_48h: identificador da sessão de conversa no chatbot.
  - cpf, telefone: identificação do cidadão que respondeu.
  - categoria: categoria atribuída pela LLM na classificação.
  - categoria_justificativa: explicação da LLM pra essa categoria.
  - resumo_gerado: resumo em 1 frase do que o cidadão pediu/tratou.
  - juiz_veredito: "CORRETO", "INCORRETO", ou vazio se essa conversa não foi
    sorteada pra auditoria do juiz.
  - juiz_categoria_esperada: preenchido só se juiz_veredito = INCORRETO — a
    categoria que o juiz considera correta.
  - juiz_justificativa: explicação do juiz pra esse veredito.
"""

from __future__ import annotations

import os
from datetime import date, datetime
from zoneinfo import ZoneInfo

from iplanrio.pipelines_utils.env import inject_bd_credentials_task
from iplanrio.pipelines_utils.logging import log
from iplanrio.pipelines_utils.prefect import rename_current_flow_run_task
from prefect import flow

from pipelines.rj_crm__relatorio_engajamento_hsm.config import (
    BIFROST_MODEL,
    DRIVE_PASTA_RAIZ_ID,
    LIMIAR_JULGA_TUDO,
    N_EXEMPLOS_POR_CATEGORIA,
    PCT_AMOSTRA_CATEGORIA_GRANDE,
    TAMANHO_LOTE_CLASSIFICACAO,
    TAMANHO_LOTE_DESCOBERTA,
    TAMANHO_LOTE_JUIZ,
    TETO_AMOSTRA_DESCOBERTA,
)
from pipelines.rj_crm__relatorio_engajamento_hsm.tasks.classify import classifica_conversas
from pipelines.rj_crm__relatorio_engajamento_hsm.tasks.extract import extrai_conversas
from pipelines.rj_crm__relatorio_engajamento_hsm.tasks.judge import julga_classificacoes
from pipelines.rj_crm__relatorio_engajamento_hsm.tasks.report import (
    ja_existe_relatorio,
    monta_e_publica_relatorio,
)
from pipelines.rj_crm__relatorio_engajamento_hsm.tasks.trigger import carrega_disparos_pendentes


def _hoje_brt() -> date:
    return datetime.now(ZoneInfo("America/Sao_Paulo")).date()


@flow(log_prints=True)
def rj_crm__relatorio_engajamento_hsm(
    data_referencia: str | None = None,
    drive_pasta_raiz_id: str = DRIVE_PASTA_RAIZ_ID,
    bifrost_model: str = BIFROST_MODEL,
    tamanho_lote_classificacao: int = TAMANHO_LOTE_CLASSIFICACAO,
    tamanho_lote_descoberta: int = TAMANHO_LOTE_DESCOBERTA,
    tamanho_lote_juiz: int = TAMANHO_LOTE_JUIZ,
    teto_amostra_descoberta: int = TETO_AMOSTRA_DESCOBERTA,
    limiar_julga_tudo: int = LIMIAR_JULGA_TUDO,
    pct_amostra_categoria_grande: float = PCT_AMOSTRA_CATEGORIA_GRANDE,
    n_exemplos_por_categoria: int = N_EXEMPLOS_POR_CATEGORIA,
) -> None:
    """Gera e publica o relatório de engajamento dos HSMs pendentes numa data de referência.

    Olha `disparos_ativos` procurando linhas com `relatorio_engajamento_data_geracao`
    igual à data de referência (por padrão, hoje) e com `nome_campanha`/
    `relatorio_engajamento_data_disparo` preenchidos. Pra cada uma: extrai as conversas
    do HSM desde a data de disparo, classifica por LLM em 2 passadas (descoberta de
    categoria + classificação), julga a classificação como LLM juiz, monta o relatório
    em .docx e publica no Drive — 1 relatório + 2 csv complementares (categorias e
    classificações, separados por ";") por rodada, na subpasta do HSM.

    Idempotente por (hsm, data de geração): rodar de novo no mesmo dia não duplica nada.

    Args:
        data_referencia: Data no formato 'YYYY-MM-DD' usada pra decidir quais disparos
            processar (comparada com relatorio_engajamento_data_geracao). None usa a
            data de hoje em America/Sao_Paulo — use um valor explícito só pra reprocessar
            manualmente uma data passada.
        drive_pasta_raiz_id: ID da pasta raiz no Google Drive onde os relatórios são
            publicados. Precisa estar compartilhada (Editor) com a service account de
            BASEDOSDADOS_CREDENTIALS_PROD. Staging pode apontar pra uma pasta de teste
            diferente da usada em produção.
        bifrost_model: Modelo (Gemini via gateway Bifrost) usado nas 3 chamadas de LLM
            do flow: descoberta de categoria, classificação e julgamento.
        tamanho_lote_classificacao: Conversas por chamada de LLM na classificação
            (passada 2 — só escolhe entre categorias já existentes no catálogo).
        tamanho_lote_descoberta: Conversas por chamada de LLM na descoberta de
            categoria (passada 1 — a única que pode criar categoria nova).
        tamanho_lote_juiz: Conversas por chamada de LLM no julgamento (LLM as a judge)
            da classificação.
        teto_amostra_descoberta: Teto de conversas amostradas aleatoriamente na
            descoberta antes de saturar — reduzir deixa uma rodada de teste mais
            barata/rápida, às custas de um catálogo de categoria menos completo.
        limiar_julga_tudo: Categoria com menos conversas que isso é julgada 100% pelo
            juiz; categoria maior é julgada só na fração de pct_amostra_categoria_grande.
        pct_amostra_categoria_grande: Fração (%) julgada em categorias que atingem
            limiar_julga_tudo — mais alto aumenta a precisão do relatório e o custo de LLM.
        n_exemplos_por_categoria: Quantos exemplos de conversa aparecem por categoria
            no .docx final — só afeta o relatório, não o custo de LLM.
    """
    rename_current_flow_run_task(new_name="relatorio_engajamento_hsm")
    inject_bd_credentials_task(environment="prod")

    bf_key = os.environ.get("BF_KEY")
    if not bf_key:
        raise ValueError(
            "BF_KEY não encontrada nas variáveis de ambiente — adicionar ao secret do work "
            "pool (mesmo secretName usado por rj_crm__agentforce_classificacao_llm)."
        )

    ref = date.fromisoformat(data_referencia) if data_referencia else _hoje_brt()

    disparos = carrega_disparos_pendentes(ref)
    if not disparos:
        log(f"[FLOW] Nenhum disparo com relatório de engajamento pedido pra {ref}.")
        return

    for disparo in disparos:
        nome_hsm = disparo["nome_hsm"]
        data_disparo = disparo["data_disparo"]

        if ja_existe_relatorio(nome_hsm, ref, drive_pasta_raiz_id):
            log(f"[FLOW] {nome_hsm}: relatório da geração {ref} já existe no Drive — pulando.")
            continue

        df_sessoes, total_disparos, total_engajados = extrai_conversas(nome_hsm, data_disparo)
        if df_sessoes.empty:
            log(f"[FLOW] {nome_hsm}: nenhuma sessão engajada desde {data_disparo} — nada a classificar.")
            continue

        df_classificada, catalogo = classifica_conversas(
            df_sessoes,
            bf_key,
            bifrost_model,
            tamanho_lote_classificacao,
            tamanho_lote_descoberta,
            teto_amostra_descoberta,
        )
        if df_classificada.empty:
            log(f"[FLOW] {nome_hsm}: nenhuma conversa classificada com sucesso — relatório não gerado.")
            continue

        df_julgada = julga_classificacoes(
            df_classificada,
            catalogo,
            bf_key,
            bifrost_model,
            tamanho_lote_juiz,
            limiar_julga_tudo,
            pct_amostra_categoria_grande,
        )
        monta_e_publica_relatorio(
            nome_hsm=nome_hsm,
            data_disparo=data_disparo,
            data_referencia=ref,
            df=df_julgada,
            catalogo=catalogo,
            total_disparos=total_disparos,
            total_engajados=total_engajados,
            drive_pasta_raiz_id=drive_pasta_raiz_id,
            n_exemplos_por_categoria=n_exemplos_por_categoria,
        )

    log("[FLOW] Concluído.")
