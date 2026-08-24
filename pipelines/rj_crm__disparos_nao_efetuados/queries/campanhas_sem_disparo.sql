-- Campanhas ativas sem disparo hoje
-- Compara as campanhas configuradas como ativas na planilha de controle
-- (raw_salesforce_disparos_ativos) com os disparos efetuados hoje
-- (raw_salesforce_disparos_efetuados_sf), trazendo as que não tiveram nenhum
-- envio no dia corrente.
-- Enriquece com o nome técnico do HSM na Meta (meta.nome_hsm) vindo de
-- brutos_salesforce.mensagem_ativa, quando disponível.
--
-- Notas:
-- - "Hoje" = date('America/Sao_Paulo') na execução.
-- - O join com mensagem_ativa é pelo nome técnico do HSM: a planilha usa
--   campanha_nome (ex: smspuerperasdisparo25) e o Content Builder usa
--   meta.nome_hsm com o mesmo valor. O join é LEFT para não perder campanhas
--   ativas cujo HSM ainda não consta no Content Builder.

with
    -- 1. Campanhas marcadas como ativas na planilha de controle (ambiente production)
    campanhas_ativas as (
        select
            campanha_nome as nome_hsm,
            nome_campanha_limpo  as nome_campanha,
            ambiente,
            id_wetalkie_hsm,
            ativo_indicador,
            limite_disparo_data,
            ingestao_datahora
        from `rj-crm-registry.brutos_salesforce.raw_disparos_ativos`
        where
            ativo_indicador is true
            and (id_wetalkie_hsm = 0 or id_wetalkie_hsm is null)
    ),

    -- 2. Disparos efetuados hoje (fuso horário de Brasília)
    disparos_hoje as (
        select distinct nome_hsm
        from `rj-crm-registry.brutos_salesforce.status_disparo`
        where date(data_particao) = current_date('America/Sao_Paulo')
    ),

    -- 3. Campanhas ativas sem disparo hoje
    resultado as (
        select
            ca.nome_campanha,
            ca.nome_hsm,
        from campanhas_ativas ca
        left join disparos_hoje
            on ca.nome_hsm = disparos_hoje.nome_hsm
        -- Exclui campanhas que já tiveram ao menos um disparo hoje
        where disparos_hoje.nome_hsm is null and ca.nome_campanha is not null
    )

select distinct nome_campanha, nome_hsm, current_date('America/Sao_Paulo') as data_particao
from resultado
order by nome_hsm
