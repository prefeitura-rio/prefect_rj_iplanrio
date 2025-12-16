



    

        

        
        

        
        
    


with
    matriz as (
        select distinct
            data_inicio_matriz,
            data_fim_matriz,
            array_to_string(sequencia_completa_modo, ', ') as modos,
            to_json_string(sequencia_completa_rateio) as rateio,
            tempo_integracao_minutos
        from `rj-smtr`.`planejamento`.`matriz_integracao`
    ),
    versao_matriz as (select distinct data_inicio_matriz, data_fim_matriz from matriz),
    integracao_agg as (
        select
            date(datetime_processamento_integracao) as data,
            id_integracao,
            string_agg(
                case
                    when modo = 'Van'
                    then consorcio
                    when modo = 'Ônibus'
                    then 'SPPO'
                    else modo
                end,
                ', '
                order by sequencia_integracao
            ) as modos,
            to_json_string(
                array_agg(
                    cast(percentual_rateio as numeric) order by sequencia_integracao
                )
            ) as rateio,
            min(datetime_transacao) as datetime_primeira_transacao,
            max(datetime_transacao) as datetime_ultima_transacao,
            min(intervalo_integracao) as menor_intervalo
        from `rj-smtr`.`br_rj_riodejaneiro_bilhetagem`.`integracao`
        
        
            where
                 data = "2000-01-01"
                
        
        group by 1, 2
    ),
    integracao_matriz as (
        select
            i.data,
            i.id_integracao,
            i.modos,
            i.rateio,
            i.datetime_primeira_transacao,
            i.datetime_ultima_transacao,
            i.menor_intervalo,
            m.modos as modos_matriz,
            m.rateio as rateio_matriz,
            m.tempo_integracao_minutos,
            v.data_inicio_matriz
        from integracao_agg i
        left join
            matriz m
            on i.data >= m.data_inicio_matriz
            and (i.data <= m.data_fim_matriz or m.data_fim_matriz is null)
            and i.modos = m.modos
        left join
            versao_matriz v
            on i.data >= v.data_inicio_matriz
            and (i.data <= v.data_fim_matriz or v.data_fim_matriz is null)
    ),
    indicadores as (
        select
            data,
            id_integracao,
            modos,
            modos_matriz is null as indicador_fora_matriz,
            case
                when modos_matriz is null
                then null
                else
                    timestamp_diff(
                        datetime_ultima_transacao, datetime_primeira_transacao, minute
                    )
                    > tempo_integracao_minutos
            end as indicador_tempo_integracao_invalido,
            case
                when modos_matriz is null then null else rateio != rateio_matriz
            end as indicador_rateio_invalido,
            rateio,
            rateio_matriz,
            data_inicio_matriz
        from integracao_matriz
    )
select
    *,
    '' as versao,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao
from indicadores
where
    (
        indicador_fora_matriz
        or indicador_tempo_integracao_invalido
        or indicador_rateio_invalido
    )
    and data >= (select min(data_inicio_matriz) from versao_matriz)