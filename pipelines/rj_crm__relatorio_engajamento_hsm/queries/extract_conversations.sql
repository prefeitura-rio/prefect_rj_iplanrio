-- Mensagens das sessões que receberam o HSM (ou a campanha), entre a data de início e a
-- data de fim dos disparos. `filter_field` é nome_hsm ou nome_campanha.
-- Fontes: HSM, AI_AGENT_CIDADAO e AI_AGENT_AGENTE (CUSTOMER fica de fora de propósito).
with seleciona_ids_por_eixo as (
    select distinct id_sessao_48h
    from `{{ project }}.{{ dataset }}.{{ table }}` c
    where c.hsm.{{ filter_field }} = {{ value }}
      and c.data_particao between {{ start_date }} and {{ end_date }}
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
from `{{ project }}.{{ dataset }}.{{ table }}` c,
unnest(mensagens) as msg
inner join seleciona_ids_por_eixo using (id_sessao_48h)
where msg.fonte in {{ sources }}
order by telefone, id_sessao_48h, msg_data
