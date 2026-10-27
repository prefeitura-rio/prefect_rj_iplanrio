-- Conversas de uma CAMPANHA num período: entram as sessões (id_sessao_48h) que
-- receberam algum HSM da campanha com data_particao entre {data_inicio} e {data_fim},
-- e cada sessão vem INTEIRA — as mensagens são buscadas até {dias_sessao} dia(s) depois
-- de {data_fim}, pra uma sessão que começou no último dia não ficar cortada no meio.
-- Mensagens HSM/AI_AGENT_CIDADAO/AI_AGENT_AGENTE (CUSTOMER fica de fora de propósito —
-- ver config.py::FONTES_CONVERSA).
--
-- Grão da rmi_conversas.chatbot: 1 linha por MENSAGEM (fonte/mensagem_* no topo, sem
-- array `mensagens`). O struct hsm só vem preenchido nas linhas fonte = 'HSM'; nas
-- AI_AGENT_* ele é nulo, e o _monta_conversas tira campanha/HSM/eixo da linha HSM.
--
-- Template com placeholders trocáveis por .format() — 1 texto só serve pra dois usos
-- (ver tasks/extract.py): _busca_mensagens roda isso com parâmetros reais do BigQuery
-- (única forma segura de executar); sql_query_utilizada usa os mesmos placeholders com
-- valor literal, só pra EXIBIR no relatório final (nunca roda contra o BQ assim).
-- Manter as duas num template só evita a query mostrada no relatório desalinhar da
-- que roda de verdade.
with sessoes_da_campanha as (
    select distinct id_sessao_48h
    from `{project}.{dataset}.{table}` c
    where c.hsm.nome_campanha = {campanha}
      and c.data_particao between {data_inicio} and {data_fim}
)
select distinct
    contato.cpf as cpf,
    contato.contato_telefone as telefone,
    id_sessao_24h,
    id_sessao_48h,
    c.hsm.nome_campanha as nome_campanha,
    c.hsm.nome_hsm as nome_hsm,
    c.mensagem_datahora as msg_data,
    c.mensagem_texto as msg_texto,
    c.fonte as msg_fonte,
    -- tipo cru do WhatsApp; hoje nulo nas falas AI_AGENT_* (= texto, ver _monta_conversas)
    c.mensagem_tipo as msg_tipo,
    c.hsm.nome_eixo as nome_eixo
from `{project}.{dataset}.{table}` c
inner join sessoes_da_campanha using (id_sessao_48h)
where c.fonte in {fontes}
  -- poda de partição: sem isso a busca das mensagens varre a tabela inteira
  and c.data_particao between {data_inicio} and date_add({data_fim}, interval {dias_sessao} day)
order by telefone, id_sessao_48h, msg_data
