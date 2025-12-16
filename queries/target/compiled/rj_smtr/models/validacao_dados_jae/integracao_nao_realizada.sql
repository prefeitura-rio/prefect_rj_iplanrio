



    

        

        
        

        
        
    


  -- Número máximo de pernas em uma integração



with
    matriz as (
        select
            data_inicio_matriz,
            data_fim_matriz,
            array_agg(array_to_string(sequencia_completa_modo, ',')) as sequencia_valida
        from `rj-smtr`.`planejamento`.`matriz_integracao`
        -- `rj-smtr.br_rj_riodejaneiro_bilhetagem.matriz_integracao`
        group by data_inicio_matriz, data_fim_matriz
    ),
    transacao as (
        select
            t.id_cliente,
            
                 datetime_transacao,
                
            
                 id_transacao,
                
            
                
                    case
                        when t.modo = 'Van'
                        then t.consorcio
                        when t.modo = 'Ônibus'
                        then 'SPPO'
                        else t.modo
                    end as modo,
                
            
                
                    concat(t.id_servico_jae, '_', t.sentido) as servico_sentido,
                
            
            m.data_inicio_matriz,
            m.sequencia_valida
        from `rj-smtr`.`br_rj_riodejaneiro_bilhetagem`.`transacao` t
        
        join
            matriz m
            on t.data >= m.data_inicio_matriz
            and (t.data <= m.data_fim_matriz or m.data_fim_matriz is null)
        where
            t.data < current_date("America/Sao_Paulo")
            and t.tipo_transacao != "Gratuidade"
            and t.id_cliente is not null
            and t.id_cliente != '733'
             and (
  
    data = "2000-01-01"
  
) 
    ),
    transacao_agrupada as (
        select
            id_cliente,
            -- Cria o conjunto de colunas para a transação atual e as 4 próximas
            -- transações do cliente
            
                
                    
                        datetime_transacao as datetime_transacao_0,
                    
                
                    
                        lead(datetime_transacao, 1) over (
                            partition by id_cliente order by datetime_transacao
                        ) as datetime_transacao_1,
                    
                
                    
                        lead(datetime_transacao, 2) over (
                            partition by id_cliente order by datetime_transacao
                        ) as datetime_transacao_2,
                    
                
                    
                        lead(datetime_transacao, 3) over (
                            partition by id_cliente order by datetime_transacao
                        ) as datetime_transacao_3,
                    
                
                    
                        lead(datetime_transacao, 4) over (
                            partition by id_cliente order by datetime_transacao
                        ) as datetime_transacao_4,
                    
                
            
                
                    
                        id_transacao as id_transacao_0,
                    
                
                    
                        lead(id_transacao, 1) over (
                            partition by id_cliente order by datetime_transacao
                        ) as id_transacao_1,
                    
                
                    
                        lead(id_transacao, 2) over (
                            partition by id_cliente order by datetime_transacao
                        ) as id_transacao_2,
                    
                
                    
                        lead(id_transacao, 3) over (
                            partition by id_cliente order by datetime_transacao
                        ) as id_transacao_3,
                    
                
                    
                        lead(id_transacao, 4) over (
                            partition by id_cliente order by datetime_transacao
                        ) as id_transacao_4,
                    
                
            
                
                    
                        modo as modo_0,
                    
                
                    
                        lead(modo, 1) over (
                            partition by id_cliente order by datetime_transacao
                        ) as modo_1,
                    
                
                    
                        lead(modo, 2) over (
                            partition by id_cliente order by datetime_transacao
                        ) as modo_2,
                    
                
                    
                        lead(modo, 3) over (
                            partition by id_cliente order by datetime_transacao
                        ) as modo_3,
                    
                
                    
                        lead(modo, 4) over (
                            partition by id_cliente order by datetime_transacao
                        ) as modo_4,
                    
                
            
                
                    
                        servico_sentido as servico_sentido_0,
                    
                
                    
                        lead(servico_sentido, 1) over (
                            partition by id_cliente order by datetime_transacao
                        ) as servico_sentido_1,
                    
                
                    
                        lead(servico_sentido, 2) over (
                            partition by id_cliente order by datetime_transacao
                        ) as servico_sentido_2,
                    
                
                    
                        lead(servico_sentido, 3) over (
                            partition by id_cliente order by datetime_transacao
                        ) as servico_sentido_3,
                    
                
                    
                        lead(servico_sentido, 4) over (
                            partition by id_cliente order by datetime_transacao
                        ) as servico_sentido_4,
                    
                
            
            data_inicio_matriz,
            sequencia_valida
        from transacao
    ),
    integracao_possivel as (
        select
            *,
            
            
            
                
                (
                    datetime_diff(
                        datetime_transacao_1,
                        datetime_transacao_0,
                        minute
                    )
                    <= 180
                    and concat(modo_0, ',', modo_1)
                    in unnest(sequencia_valida)
                    
                        and servico_sentido_1
                        != servico_sentido_0
                    
                ) as indicador_integracao_1,
                

            
                
                (
                    datetime_diff(
                        datetime_transacao_2,
                        datetime_transacao_0,
                        minute
                    )
                    <= 180
                    and concat(modo_0, ',', modo_1, ',', modo_2)
                    in unnest(sequencia_valida)
                    
                        and servico_sentido_2
                        not in (servico_sentido_0, ',', servico_sentido_1)
                    
                ) as indicador_integracao_2,
                

            
                
                (
                    datetime_diff(
                        datetime_transacao_3,
                        datetime_transacao_0,
                        minute
                    )
                    <= 180
                    and concat(modo_0, ',', modo_1, ',', modo_2, ',', modo_3)
                    in unnest(sequencia_valida)
                    
                        and servico_sentido_3
                        not in (servico_sentido_0, ',', servico_sentido_1, ',', servico_sentido_2)
                    
                ) as indicador_integracao_3,
                

            
                
                (
                    datetime_diff(
                        datetime_transacao_4,
                        datetime_transacao_0,
                        minute
                    )
                    <= 180
                    and concat(modo_0, ',', modo_1, ',', modo_2, ',', modo_3, ',', modo_4)
                    in unnest(sequencia_valida)
                    
                        and servico_sentido_4
                        not in (servico_sentido_0, ',', servico_sentido_1, ',', servico_sentido_2, ',', servico_sentido_3)
                    
                ) as indicador_integracao_4,
                

            
        from transacao_agrupada
        where id_transacao_1 is not null
    ),
    transacao_filtrada as (
        select
            id_cliente,
            
                
                    
                        datetime_transacao_0,
                    
                
                    
                        datetime_transacao_1,
                    
                
                    
                        case
                            when indicador_integracao_2
                            then datetime_transacao_2
                        end as datetime_transacao_2,
                    
                
                    
                        case
                            when indicador_integracao_3
                            then datetime_transacao_3
                        end as datetime_transacao_3,
                    
                
                    
                        case
                            when indicador_integracao_4
                            then datetime_transacao_4
                        end as datetime_transacao_4,
                    
                
            
                
                    
                        id_transacao_0,
                    
                
                    
                        id_transacao_1,
                    
                
                    
                        case
                            when indicador_integracao_2
                            then id_transacao_2
                        end as id_transacao_2,
                    
                
                    
                        case
                            when indicador_integracao_3
                            then id_transacao_3
                        end as id_transacao_3,
                    
                
                    
                        case
                            when indicador_integracao_4
                            then id_transacao_4
                        end as id_transacao_4,
                    
                
            
                
                    
                        modo_0,
                    
                
                    
                        modo_1,
                    
                
                    
                        case
                            when indicador_integracao_2
                            then modo_2
                        end as modo_2,
                    
                
                    
                        case
                            when indicador_integracao_3
                            then modo_3
                        end as modo_3,
                    
                
                    
                        case
                            when indicador_integracao_4
                            then modo_4
                        end as modo_4,
                    
                
            
                
                    
                        servico_sentido_0,
                    
                
                    
                        servico_sentido_1,
                    
                
                    
                        case
                            when indicador_integracao_2
                            then servico_sentido_2
                        end as servico_sentido_2,
                    
                
                    
                        case
                            when indicador_integracao_3
                            then servico_sentido_3
                        end as servico_sentido_3,
                    
                
                    
                        case
                            when indicador_integracao_4
                            then servico_sentido_4
                        end as servico_sentido_4,
                    
                
            
            indicador_integracao_1,
            indicador_integracao_2,
            indicador_integracao_3,
            indicador_integracao_4,
            data_inicio_matriz
        from integracao_possivel
        where indicador_integracao_1
    ),
    transacao_listada as (
        select
            *,
            array_to_string(
                [
                    
                        id_transacao_1 ,
                    
                        id_transacao_2 ,
                    
                        id_transacao_3 ,
                    
                        id_transacao_4 
                    
                ],
                ", "
            ) as transacoes
        from transacao_filtrada
    ),
    
        validacao_integracao_5_pernas as (
            select
                (
                    id_transacao_4 is not null
                    
                    and id_transacao_0 in unnest(
                        split(
                            string_agg(transacoes, ", ") over (
                                partition by id_cliente
                                order by datetime_transacao_0
                                rows between 5 preceding and 1 preceding
                            ),
                            ', '
                        )
                    )
                ) as remover_5,
                *
            from  transacao_listada
            
        ),
    
        validacao_integracao_4_pernas as (
            select
                (
                    id_transacao_3 is not null
                    
                        and id_transacao_4 is null
                    
                    and id_transacao_0 in unnest(
                        split(
                            string_agg(transacoes, ", ") over (
                                partition by id_cliente
                                order by datetime_transacao_0
                                rows between 5 preceding and 1 preceding
                            ),
                            ', '
                        )
                    )
                ) as remover_4,
                *
            from 
                    validacao_integracao_5_pernas
                where not remover_5
            
        ),
    
        validacao_integracao_3_pernas as (
            select
                (
                    id_transacao_2 is not null
                    
                        and id_transacao_3 is null
                    
                    and id_transacao_0 in unnest(
                        split(
                            string_agg(transacoes, ", ") over (
                                partition by id_cliente
                                order by datetime_transacao_0
                                rows between 5 preceding and 1 preceding
                            ),
                            ', '
                        )
                    )
                ) as remover_3,
                *
            from 
                    validacao_integracao_4_pernas
                where not remover_4
            
        ),
    
        validacao_integracao_2_pernas as (
            select
                (
                    id_transacao_1 is not null
                    
                        and id_transacao_2 is null
                    
                    and id_transacao_0 in unnest(
                        split(
                            string_agg(transacoes, ", ") over (
                                partition by id_cliente
                                order by datetime_transacao_0
                                rows between 5 preceding and 1 preceding
                            ),
                            ', '
                        )
                    )
                ) as remover_2,
                *
            from 
                    validacao_integracao_3_pernas
                where not remover_3
            
        ),
    
    integracoes_validas as (
        select date(datetime_transacao_0) as data, id_transacao_0 as id_integracao, *
        from validacao_integracao_2_pernas
        where not remover_2
    ),
    melted as (
        select
            data,
            id_integracao,
            sequencia_integracao,
            datetime_transacao,
            id_transacao,
            modo,
            split(servico_sentido, '_')[0] as id_servico_jae,
            split(servico_sentido, '_')[1] as sentido,
            countif(modo = "BRT") over (partition by id_integracao)
            > 1 as indicador_transferencia_brt,
            countif(modo = "VLT") over (partition by id_integracao)
            > 1 as indicador_transferencia_vlt,
            data_inicio_matriz
        from
            integracoes_validas,
            unnest(
                [
                    
                        struct(
                            
                                datetime_transacao_0 as datetime_transacao,
                            
                                id_transacao_0 as id_transacao,
                            
                                modo_0 as modo,
                            
                                servico_sentido_0 as servico_sentido,
                            
                            1 as sequencia_integracao
                        )
                        ,
                    
                        struct(
                            
                                datetime_transacao_1 as datetime_transacao,
                            
                                id_transacao_1 as id_transacao,
                            
                                modo_1 as modo,
                            
                                servico_sentido_1 as servico_sentido,
                            
                            2 as sequencia_integracao
                        )
                        ,
                    
                        struct(
                            
                                datetime_transacao_2 as datetime_transacao,
                            
                                id_transacao_2 as id_transacao,
                            
                                modo_2 as modo,
                            
                                servico_sentido_2 as servico_sentido,
                            
                            3 as sequencia_integracao
                        )
                        ,
                    
                        struct(
                            
                                datetime_transacao_3 as datetime_transacao,
                            
                                id_transacao_3 as id_transacao,
                            
                                modo_3 as modo,
                            
                                servico_sentido_3 as servico_sentido,
                            
                            4 as sequencia_integracao
                        )
                        ,
                    
                        struct(
                            
                                datetime_transacao_4 as datetime_transacao,
                            
                                id_transacao_4 as id_transacao,
                            
                                modo_4 as modo,
                            
                                servico_sentido_4 as servico_sentido,
                            
                            5 as sequencia_integracao
                        )
                        
                    
                ]
            )
    ),
    integracao_nao_realizada as (
        select distinct id_integracao
        from melted
        where
            not indicador_transferencia_brt
            and not indicador_transferencia_vlt
            and id_transacao not in (
                select id_transacao
                from `rj-smtr`.`br_rj_riodejaneiro_bilhetagem`.`integracao`
                -- `rj-smtr.br_rj_riodejaneiro_bilhetagem.integracao`
                
                    where 
  
    data = "2000-01-01"
  

                
            )
    )
select
    * except (indicador_transferencia_brt, indicador_transferencia_vlt),
    '' as versao,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao
from melted
where
    id_integracao in (select id_integracao from integracao_nao_realizada)
    and id_transacao is not null