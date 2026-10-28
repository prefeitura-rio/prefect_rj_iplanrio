-- Conversas de um HSM (template) num período, montadas direto dos eventos de WhatsApp
-- (modo fonte_disparos = "hsm", só por custom run — ver flow.py), sem passar pela
-- rmi_conversas.chatbot nem pela planilha de campanhas:
--   1. disparos = SEND_STATUS SENT de MARKETING_CLOUD_ENGAGEMENT com $.template.name =
--      {hsm} e data do envio entre {data_inicio} e {data_fim};
--   2. conversa = eventos DIGITAL_ENGAGEMENT do mesmo telefone até {horas_janela}h
--      depois do envio: INBOUND = fala do cidadão (inclui clique em botão do HSM),
--      SEND_STATUS = fala do agente de IA.
-- Mesmo formato de saída de extrai_conversas.sql — o _monta_conversas
-- (tasks/extract.py) serve aos dois. msg_tipo é o tipo cru do WhatsApp (text, button,
-- reaction, audio...); o agrupamento em tipos de engajamento fica no Python. nome_campanha/nome_eixo saem nulos: este modo
-- não conhece campanha.
--
-- Lê as tabelas de brutos_salesforce_staging porque a raw do dbt
-- (brutos_salesforce.messaging_events_whatsapp) filtra só MARKETING e descarta o
-- DIGITAL_ENGAGEMENT, e a raw do send_event não traz message_text.
--
-- Sessão: 1 por (telefone, envio). Reenvio do MESMO HSM pro mesmo telefone dentro da
-- janela (acontece: 2 envios com segundos de diferença) é absorvido pelo envio
-- anterior, senão o primeiro contaria como disparo sem resposta. Uma mensagem do DE vai
-- pro envio MAIS RECENTE de qualquer HSM antes dela pro mesmo telefone: se o cidadão
-- recebeu outro HSM no meio da janela, o que vem depois é resposta àquele.
--
-- Horário: event_date_time vem nulo em ~0,1% dos eventos; cai pro timestamp do
-- webhook da Meta (INBOUND) ou pra _first_loaded_at.
--
-- Mesmo esquema de placeholders de extrai_conversas.sql (parâmetro do BQ pra rodar,
-- valor literal só pra exibir no relatório) — ver tasks/extract.py.
with
    eventos as (
        select
            event_id,
            application,
            event_type,
            message_id,
            regexp_replace(contact_point_phone_number, r'[^0-9]', '') as telefone,
            coalesce(
                datetime(event_date_time, 'America/Sao_Paulo'),
                datetime(
                    timestamp_seconds(
                        safe_cast(
                            json_extract_scalar(message, '$.entry[0].changes[0].value.messages[0].timestamp')
                            as int64
                        )
                    ),
                    'America/Sao_Paulo'
                ),
                datetime(_first_loaded_at, 'America/Sao_Paulo')
            ) as evento_datahora,
            message
        from `{tabela_eventos}`
        where event_type in ('SEND_STATUS', 'INBOUND')
          and (send_status = 'SENT' or event_type = 'INBOUND')
          and application in ('MARKETING_CLOUD_ENGAGEMENT', 'DIGITAL_ENGAGEMENT')
          -- poda de partição com 1 dia de folga (data_particao é dia de carga)
          and data_particao between date_sub({data_inicio}, interval 1 day)
              and date_add({data_fim}, interval {dias_sessao} + 1 day)
        -- recaptura: mesmo evento em mais de uma carga
        qualify row_number() over (partition by event_id order by _loaded_at desc) = 1
    ),

    -- envios de template da Marketing Cloud (qualquer HSM): os de outros HSMs entram
    -- só pra disputar a atribuição das mensagens do DE
    envios as (
        select
            message_id,
            json_extract_scalar(message, '$.template.name') as nome_hsm,
            telefone,
            evento_datahora as envio_datahora
        from eventos
        where application = 'MARKETING_CLOUD_ENGAGEMENT'
          and event_type = 'SEND_STATUS'
          and json_extract_scalar(message, '$.template.name') is not null
    ),

    -- reenvio do mesmo HSM pro mesmo telefone dentro da janela: fica só o 1º
    envios_sem_reenvio as (
        select *
        from envios
        qualify
            coalesce(
                lag(envio_datahora) over (partition by telefone, nome_hsm order by envio_datahora)
                < datetime_sub(envio_datahora, interval {horas_janela} hour),
                true
            )
    ),

    disparos_do_hsm as (
        select *
        from envios_sem_reenvio
        where nome_hsm = {hsm}
          and date(envio_datahora) between {data_inicio} and {data_fim}
    ),

    -- fala do DE em texto puro, no formato do prompt (1 linha "CIDADÃO:"/"AGENTE:"
    -- por mensagem). Mídia vira marcador entre colchetes; menu do agente leva as
    -- opções, pra a escolha do cidadão fazer sentido
    mensagens_de as (
        select
            event_id,
            telefone,
            evento_datahora as msg_data,
            if(event_type = 'INBOUND', 'AI_AGENT_CIDADAO', 'AI_AGENT_AGENTE') as msg_fonte,
            coalesce(
                json_extract_scalar(message, '$.entry[0].changes[0].value.messages[0].type'),
                json_extract_scalar(message, '$.type')
            ) as msg_tipo,
            case
                when event_type = 'INBOUND' then (
                    select
                        case json_extract_scalar(m, '$.type')
                            when 'text' then json_extract_scalar(m, '$.text.body')
                            when 'button' then json_extract_scalar(m, '$.button.text')
                            when 'interactive' then coalesce(
                                json_extract_scalar(m, '$.interactive.button_reply.title'),
                                json_extract_scalar(m, '$.interactive.list_reply.title'),
                                '[resposta de formulário]'
                            )
                            when 'audio' then '[áudio]'
                            when 'image' then trim(concat('[imagem] ', coalesce(json_extract_scalar(m, '$.image.caption'), '')))
                            when 'video' then trim(concat('[vídeo] ', coalesce(json_extract_scalar(m, '$.video.caption'), '')))
                            when 'document' then '[documento]'
                            when 'sticker' then '[figurinha]'
                            when 'contacts' then '[contato]'
                            -- sem emoji = o cidadão desfez a reação: descartada
                            when 'reaction' then concat('[reação: ', json_extract_scalar(m, '$.reaction.emoji'), ']')
                        end
                    from (select json_extract(message, '$.entry[0].changes[0].value.messages[0]') as m)
                )
                else
                    case json_extract_scalar(message, '$.type')
                        when 'text' then json_extract_scalar(message, '$.text.body')
                        when 'interactive' then concat(
                            coalesce(json_extract_scalar(message, '$.interactive.body.text'), ''),
                            '\n[opções: ',
                            array_to_string(
                                array_concat(
                                    array(
                                        select json_extract_scalar(b, '$.reply.title')
                                        from unnest(json_extract_array(message, '$.interactive.action.buttons')) b
                                    ),
                                    array(
                                        select json_extract_scalar(r, '$.title')
                                        from unnest(json_extract_array(message, '$.interactive.action.sections')) s,
                                            unnest(json_extract_array(s, '$.rows')) r
                                    )
                                ),
                                ' | '
                            ),
                            ']'
                        )
                        when 'audio' then '[áudio]'
                        when 'image' then '[imagem]'
                        when 'document' then '[documento]'
                        when 'template' then '[formulário]'
                    end
            end as msg_texto
        from eventos
        where application = 'DIGITAL_ENGAGEMENT'
    ),

    respostas as (
        select a.msg_data, a.msg_texto, a.msg_fonte, a.msg_tipo, e.message_id
        from mensagens_de a
        inner join envios_sem_reenvio e
            on e.telefone = a.telefone
            and a.msg_data >= e.envio_datahora
            and a.msg_data < datetime_add(e.envio_datahora, interval {horas_janela} hour)
        -- sem texto = indicador de digitação/status, reação desfeita, tipo não suportado
        where a.msg_texto is not null
        qualify
            row_number() over (partition by a.event_id order by e.envio_datahora desc) = 1
    ),

    -- texto do template (variáveis sem preencher) e CPF (subscriber_key) do disparo;
    -- message_key = message_id do evento
    send_event as (
        select
            message_key as message_id,
            any_value(message_text) as hsm_texto,
            any_value(if(regexp_contains(subscriber_key, r'^[0-9]{{11}}$'), subscriber_key, null)) as cpf
        from `{tabela_send_event}`
        where data_particao between date_sub({data_inicio}, interval 1 day)
            and date_add({data_fim}, interval 1 day)
        group by 1
    ),

    mensagens as (
        select
            d.message_id,
            d.envio_datahora as msg_data,
            se.hsm_texto as msg_texto,
            'HSM' as msg_fonte,
            'template' as msg_tipo
        from disparos_do_hsm d
        left join send_event se using (message_id)
        union all
        select message_id, msg_data, msg_texto, msg_fonte, msg_tipo
        from respostas
    )

select
    se.cpf,
    d.telefone,
    d.message_id as id_sessao_24h,
    d.message_id as id_sessao_48h,
    cast(null as string) as nome_campanha,
    d.nome_hsm,
    m.msg_data,
    m.msg_texto,
    m.msg_fonte,
    m.msg_tipo,
    cast(null as string) as nome_eixo
from mensagens m
inner join disparos_do_hsm d using (message_id)
left join send_event se using (message_id)
order by telefone, id_sessao_48h, msg_data
