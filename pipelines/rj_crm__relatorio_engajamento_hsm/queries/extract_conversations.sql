-- Mensagens das sessões que receberam o HSM, a partir da data de início.
-- Fontes: HSM, AI_AGENT_CIDADAO e AI_AGENT_AGENTE (CUSTOMER fica de fora de propósito).
with seleciona_ids_por_eixo as (
    select distinct id_sessao_48h
    from `{{ project }}.{{ dataset }}.{{ table }}` c
    where c.hsm.nome_hsm = {{ hsm_name }}
      and c.data_particao >= {{ start_date }}
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
