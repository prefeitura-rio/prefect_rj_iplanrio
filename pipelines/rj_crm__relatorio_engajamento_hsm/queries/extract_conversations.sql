-- The conversations of a campaign or an HSM in a period, from rmi_conversas.chatbot.
-- A conversation is a receptive session (id_sessao_receptivo) that started between the
-- start and the end date, and whose HSM (id_sessao_ativo_origem) has
-- hsm.{{ filter_field }} = the value. `filter_field` is nome_campanha or nome_hsm.
-- Each session comes whole, with one row from the source HSM in front, so the pipeline
-- takes the HSM text from it. The sources are HSM, AI_AGENT_CIDADAO and AI_AGENT_AGENTE:
-- CUSTOMER stays out on purpose.
-- The table is flat, one row for each message.
with
    sessions as (
        select distinct
            c.id_sessao_receptivo,
            c.receptivo_inicio_datahora,
            h.contato.cpf,
            h.contato.contato_telefone as telefone,
            h.hsm.nome_campanha,
            h.hsm.nome_eixo,
            h.mensagem_texto as hsm_texto
        from `{{ project }}.{{ dataset }}.{{ table }}` c
        inner join `{{ project }}.{{ dataset }}.{{ table }}` h on h.id_mensagem = c.id_sessao_ativo_origem
        where c.data_particao between {{ start_date }} and {{ end_date }}
          and date(c.receptivo_inicio_datahora) between {{ start_date }} and {{ end_date }}
          and h.hsm.{{ filter_field }} = {{ value }}
    ),

    messages as (
        select
            id_sessao_receptivo,
            receptivo_inicio_datahora as msg_data,
            hsm_texto as msg_texto,
            'HSM' as msg_fonte
        from sessions
        union all
        select
            id_sessao_receptivo,
            mensagem_datahora,
            case
                -- a mark, so the model does not read a reaction as speech
                when mensagem_tipo = 'reaction' then concat('[reação: ', mensagem_texto, ']')
                -- media with no caption has no text: the type stays in brackets
                else coalesce(mensagem_texto, concat('[', mensagem_tipo, ']'))
            end,
            fonte
        from `{{ project }}.{{ dataset }}.{{ table }}`
        -- one more day: a session that starts near midnight of the last day
        where data_particao between {{ start_date }} and date_add({{ end_date }}, interval 1 day)
          and id_sessao_receptivo in (select id_sessao_receptivo from sessions)
          -- a reaction with no emoji means that the citizen took the reaction back
          and not (mensagem_tipo = 'reaction' and mensagem_texto is null)
          and fonte in {{ sources }}
    )

select
    s.cpf,
    s.telefone,
    s.id_sessao_receptivo as id_sessao_24h,
    s.id_sessao_receptivo as id_sessao_48h,
    s.nome_campanha,
    m.msg_data,
    m.msg_texto,
    m.msg_fonte,
    s.nome_eixo
from messages m
inner join sessions s using (id_sessao_receptivo)
order by telefone, id_sessao_48h, msg_data
