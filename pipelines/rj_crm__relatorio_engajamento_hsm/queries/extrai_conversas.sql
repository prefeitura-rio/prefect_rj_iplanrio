-- Conversas de uma CAMPANHA ou de um HSM (template) num período, da
-- rmi_conversas.chatbot: as sessões receptivas (id_sessao_receptivo) que começaram
-- entre {data_inicio} e {data_fim} e cujo HSM associado (id_sessao_ativo_origem) tem
-- hsm.{campo_disparo} = {filtro} — nome_campanha no modo do Form, nome_hsm no modo hsm
-- (fonte_disparos, ver flow.py). Cada sessão vem inteira, com 1 linha 'HSM' na frente
-- pro _monta_conversas (tasks/extract.py) tirar campanha, HSM e texto do template.
--
-- Template com placeholders trocáveis por .format() — 1 texto só serve pra dois usos
-- (ver tasks/extract.py): _busca_mensagens roda isso com parâmetros reais do BigQuery
-- (única forma segura de executar); sql_query_utilizada usa os mesmos placeholders com
-- valor literal, só pra EXIBIR no relatório final (nunca roda contra o BQ assim).
-- Manter as duas num template só evita a query mostrada no relatório desalinhar da
-- que roda de verdade.
with
    sessoes as (
        select distinct
            c.id_sessao_receptivo,
            c.receptivo_inicio_datahora,
            h.contato.cpf,
            h.contato.contato_telefone as telefone,
            h.hsm.nome_campanha,
            h.hsm.nome_hsm,
            h.hsm.nome_eixo,
            h.mensagem_texto as hsm_texto
        from `{tabela_chatbot}` c
        inner join `{tabela_chatbot}` h on h.id_mensagem = c.id_sessao_ativo_origem
        where c.data_particao between {data_inicio} and {data_fim}
          and date(c.receptivo_inicio_datahora) between {data_inicio} and {data_fim}
          and h.hsm.{campo_disparo} = {filtro}
    ),

    mensagens as (
        select id_sessao_receptivo, receptivo_inicio_datahora as msg_data, hsm_texto as msg_texto, 'HSM' as msg_fonte, 'template' as msg_tipo
        from sessoes
        union all
        select
            id_sessao_receptivo,
            mensagem_datahora,
            case
                -- marcador pra LLM não ler a reação como fala (e _emoji_reacao contar)
                when mensagem_tipo = 'reaction' then concat('[reação: ', mensagem_texto, ']')
                -- mídia sem legenda/transcrição vem sem texto: fica o tipo entre colchetes
                else coalesce(mensagem_texto, concat('[', mensagem_tipo, ']'))
            end,
            fonte,
            mensagem_tipo
        from `{tabela_chatbot}`
        -- +1 dia: sessão que começa perto da meia-noite do último dia
        where data_particao between {data_inicio} and date_add({data_fim}, interval 1 day)
          and id_sessao_receptivo in (select id_sessao_receptivo from sessoes)
          -- reação sem emoji = o cidadão desfez a reação
          and not (mensagem_tipo = 'reaction' and mensagem_texto is null)
    )

select
    s.cpf,
    s.telefone,
    s.id_sessao_receptivo as id_sessao_24h,
    s.id_sessao_receptivo as id_sessao_48h,
    s.nome_campanha,
    s.nome_hsm,
    m.msg_data,
    m.msg_texto,
    m.msg_fonte,
    m.msg_tipo,
    s.nome_eixo
from mensagens m
inner join sessoes s using (id_sessao_receptivo)
order by telefone, id_sessao_48h, msg_data
