






    
        

        

        
    


with
    rdo_new as (
        select
            data_transacao,
            data_particao as data_arquivo_rdo,
            trim(linha) as servico_riocard,
            trim(linha_rcti) as linha_riocard,
            trim(operadora) as operadora,
            gratuidade_idoso as quantidade_transacao_gratuidade_idoso,
            gratuidade_especial as quantidade_transacao_gratuidade_especial,
            gratuidade_estudante_federal
            as quantidade_transacao_gratuidade_estudante_federal,
            gratuidade_estudante_estadual
            as quantidade_transacao_gratuidade_estudante_estadual,
            gratuidade_estudante_municipal
            as quantidade_transacao_gratuidade_estudante_municipal,
            universitario as quantidade_transacao_buc_universitario,
            buc_1a_perna as quantidade_transacao_buc_perna_1,
            buc_2a_perna as quantidade_transacao_buc_perna_2,
            buc_receita as valor_buc,
            buc_supervia_1a_perna as quantidade_transacao_buc_supervia_perna_1,
            buc_supervia_2a_perna as quantidade_transacao_buc_supervia_perna_2,
            buc_supervia_receita as valor_buc_supervia,
            buc_van_1a_perna as quantidade_transacao_buc_van_perna_1,
            buc_van_2a_perna as quantidade_transacao_buc_van_perna_2,
            buc_van_receita as valor_buc_van,
            buc_brt_1a_perna as quantidade_transacao_buc_brt_perna_1,
            buc_brt_2a_perna as quantidade_transacao_buc_brt_perna_2,
            buc_brt_3a_perna as quantidade_transacao_buc_brt_perna_3,
            buc_brt_receita as valor_buc_brt,
            buc_inter_1a_perna as quantidade_transacao_buc_inter_perna_1,
            buc_inter_2a_perna as quantidade_transacao_buc_inter_perna_2,
            buc_inter_receita as valor_buc_inter,
            buc_metro_1a_perna as quantidade_transacao_buc_metro_perna_1,
            buc_metro_2a_perna as quantidade_transacao_buc_metro_perna_2,
            buc_metro_receita as valor_buc_metro,
            cartao as quantidade_transacao_cartao,
            receita_cartao as valor_cartao,
            especie_passageiro_transportado as quantidade_transacao_especie,
            especie_receita as valor_especie,
            data_processamento,
            timestamp_captura as datetime_captura
        from `rj-smtr`.`br_rj_riodejaneiro_rdo_staging`.`rdo_registros_stpl`
         where 
ano BETWEEN
    EXTRACT(YEAR FROM DATE("2022-01-01T00:00:00"))
    AND EXTRACT(YEAR FROM DATE("2022-01-01T01:00:00"))
AND mes BETWEEN
    EXTRACT(MONTH FROM DATE("2022-01-01T00:00:00"))
    AND EXTRACT(MONTH FROM DATE("2022-01-01T01:00:00"))
AND dia BETWEEN
    EXTRACT(DAY FROM DATE("2022-01-01T00:00:00"))
    AND EXTRACT(DAY FROM DATE("2022-01-01T01:00:00"))
 
    ),
    rdo_complete_partitions as (
        select *
        from rdo_new

        

            union all

            select * except (versao, datetime_ultima_atualizacao)
            from `rj-smtr`.`br_rj_riodejaneiro_rdo`.`rdo_registros_stpl`
            where data_transacao in ('2021-12-20', '2021-12-21', '2021-12-26', '2021-12-27', '2021-12-13', '2021-12-14', '2021-12-15', '2021-12-17', '2021-12-22', '2021-12-23', '2021-12-18', '2021-12-24', '2021-12-16', '2021-12-25', '2021-12-19', '2021-12-04', '2021-11-18')

        
    ),
    aux_dedup as (
        select data_arquivo_rdo, max(datetime_captura) as max_datetime_captura
        from rdo_complete_partitions
        group by 1
    )
select
    r.*,
    '' as versao,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao
from rdo_complete_partitions r
join aux_dedup a using (data_arquivo_rdo)
where a.max_datetime_captura = r.datetime_captura