-- Todas as sessões (id_sessao_48h) que receberam o HSM indicado a partir de
-- {data_inicio}, com as mensagens HSM/AI_AGENT_CIDADAO/AI_AGENT_AGENTE de cada uma
-- (CUSTOMER fica de fora de propósito — ver config.py::FONTES_CONVERSA).
--
-- Template com placeholders trocáveis por .format() — 1 texto só serve pra dois usos
-- (ver tasks/extract.py): busca_mensagens roda isso com @nome_hsm/@data_inicio/@fontes
-- como parâmetro real do BigQuery (única forma segura de executar); sql_query_utilizada
-- usa os mesmos placeholders com valor literal, só pra EXIBIR no relatório final (nunca
-- roda contra o BQ assim). Manter as duas num template só evita a query mostrada no
-- relatório desalinhar da que roda de verdade.
with seleciona_ids_por_eixo as (
    select distinct id_sessao_48h
    from `{project}.{dataset}.{table}` c
    where c.hsm.nome_hsm = {nome_hsm}
      and c.data_particao >= {data_inicio}
)
select distinct
    contato.cpf as cpf,
    contato.contato_telefone as telefone,
    id_sessao_24h,
    id_sessao_48h,
    c.hsm.nome_campanha as nome_campanha,
    msg.data as msg_data,
    msg.texto as msg_texto,
    msg.fonte as msg_fonte,
    nome_eixo
from `{project}.{dataset}.{table}` c,
unnest(mensagens) as msg
inner join seleciona_ids_por_eixo using (id_sessao_48h)
where msg.fonte in {fontes}
order by telefone, id_sessao_48h, msg_data
