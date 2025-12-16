




    
        
        
    


with
    infracoes_renainf as (
        select
            concat(codigo_infracao, desdobramento) as codigo_enquadramento,
            descricao_infracao as tipificacao_resumida,
            amparo_legal
        from `rj-smtr-staging`.`transito_staging`.`infracoes_renainf`
    ),
    autuacao_ids as (
        select data, id_autuacao, id_auto_infracao, fonte
        from `rj-smtr`.`transito_staging`.`aux_autuacao_id`
         where 
    date(data)
    between date("2022-01-01T00:00:00") and date("2022-01-01T01:00:00")
 
    ),
    citran as (
        select
            data_autuacao as data,
            id_auto_infracao,
            datetime(concat(data, ' ', hora, ':00')) as datetime_autuacao,
            data_limite_defesa_previa,
            data_limite_recurso,
            situacao_atual as descricao_situacao_autuacao,
            if(status_infracao != "", status_infracao, null) as status_infracao,
            replace(codigo_enquadramento, '-', '') as codigo_enquadramento,
            safe_cast(
                substr(regexp_extract(pontuacao, r'\d+'), 2) as string
            ) as pontuacao,
            case
                when pontuacao = "03"
                then 'Leve'
                when
                    initcap(regexp_replace(pontuacao, r'\d+', '')) = 'Media'
                    or pontuacao = "04"
                then 'Média'
                when pontuacao = "05"
                then 'Grave'
                when
                    initcap(regexp_replace(pontuacao, r'\d+', '')) = 'Gravissima'
                    or pontuacao = "07"
                then 'Gravíssima'
                else initcap(regexp_replace(pontuacao, r'\d+', ''))
            end as gravidade,
            initcap(tipo_veiculo) as tipo_veiculo,
            if(descricao_veiculo != "", descricao_veiculo, null) as descricao_veiculo,
            safe_cast(null as string) as placa_veiculo,
            safe_cast(null as string) as ano_fabricacao_veiculo,
            safe_cast(null as string) as ano_modelo_veiculo,
            safe_cast(null as string) as cor_veiculo,
            case
                when
                    initcap(regexp_replace(especie_veiculo, r'\d+', ''))
                    in ('Misto', '0Misto')
                then 'Misto'
                when
                    initcap(regexp_replace(especie_veiculo, r'\d+', ''))
                    in ('Passageir', '0Passageir', 'Passageiro', '0Passageiro')
                then 'Passageiro'
                when
                    initcap(regexp_replace(especie_veiculo, r'\d+', ''))
                    in ('Tracao', '0Tracao', 'Tracao')
                then 'Tração'
                when
                    initcap(regexp_replace(especie_veiculo, r'\d+', ''))
                    in ('Nao Inform', '0Nao Inform', 'Nao Informado', '0Nao Informado')
                then 'Não informado'
                when
                    initcap(regexp_replace(especie_veiculo, r'\d+', ''))
                    in ('Carga', '0Carga')
                then 'Carga'
                else 'Inválido'
            end as especie_veiculo,
            safe_cast(null as string) as uf_infrator,
            safe_cast(null as string) as uf_principal_condutor,
            if(uf_proprietario != "", uf_proprietario, null) as uf_proprietario,
            if(cep_proprietario != "", cep_proprietario, null) as cep_proprietario,
            valor_infracao / 100 as valor_infracao,
            valor_pago / 100 as valor_pago,
            data_pagamento,
            "260010" as id_autuador,
            if(
                descricao_autuador != "", descricao_autuador, null
            ) as descricao_autuador,
            "6001" as id_municipio_autuacao,
            "RIO DE JANEIRO" as descricao_municipio,
            "RJ" as uf_autuacao,
            endereco_autuacao,
            null as tile_autuacao,
            if(
                processo_defesa_autuacao != "00000000"
                and processo_defesa_autuacao != "",
                processo_defesa_autuacao,
                null
            ) as processo_defesa_autuacao,
            if(
                recurso_penalidade_multa != "00000000"
                and recurso_penalidade_multa != "",
                recurso_penalidade_multa,
                null
            ) as recurso_penalidade_multa,
            if(
                processo_troca_real_infrator != "00000000"
                and processo_troca_real_infrator != "",
                processo_troca_real_infrator,
                null
            ) as processo_troca_real_infrator,
            false as status_sne,
            "CITRAN" as fonte,
            current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao
        from `rj-smtr`.`transito_staging`.`autuacao_citran`
         where 
    date(data)
    between date("2022-01-01T00:00:00") and date("2022-01-01T01:00:00")
 
    ),
    serpro as (
        select
            data_autuacao as data,
            id_auto_infracao,
            datetime_autuacao,
            data_limite_defesa_previa,
            data_limite_recurso,
            descricao_situacao_autuacao,
            if(status_infracao != "", status_infracao, null) as status_infracao,
            concat(codigo_enquadramento, codigo_desdobramento) as codigo_enquadramento,
            substr(pontuacao, 1, 1) as pontuacao,
            gravidade,
            initcap(tipo_veiculo) as tipo_veiculo,
            if(descricao_veiculo != "", descricao_veiculo, null) as descricao_veiculo,
            placa_veiculo,
            ano_fabricacao_veiculo,
            ano_modelo_veiculo,
            cor_veiculo,
            especie_veiculo,
            uf_infrator,
            uf_principal_condutor,
            if(uf_proprietario != "", uf_proprietario, null) as uf_proprietario,
            safe_cast(null as string) as cep_proprietario,
            valor_infracao,
            valor_pago,
            data_pagamento,
            coalesce(id_autuador, "260010") as id_autuador,
            if(
                descricao_autuador != "", descricao_autuador, null
            ) as descricao_autuador,
            coalesce(id_municipio_autuacao, "6001") as id_municipio_autuacao,
            coalesce(descricao_municipio, "RIO DE JANEIRO") as descricao_municipio,
            coalesce(uf_autuacao, "RJ") as uf_autuacao,
            case
                when logradouro_autuacao is not null
                then
                    rtrim(
                        regexp_replace(
                            concat(
                                logradouro_autuacao,
                                ' ',
                                bairro_autuacao,
                                ' ',
                                complemento
                            ),
                            r'\s+',
                            ' '
                        )
                    )
                when logradouro_rodovia_autuacao is not null
                then logradouro_rodovia_autuacao
                else null
            end as endereco_autuacao,
            null as tile_autuacao,
            processo_defesa_autuacao,
            recurso_penalidade_multa,
            processo_troca_real_infrator,
            if(status_sne = "1.0", true, false) as status_sne,
            "SERPRO" as fonte,
            current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao
        from `rj-smtr`.`transito_staging`.`autuacao_serpro`
         where 
    date(data)
    between date("2022-01-01T00:00:00") and date("2022-01-01T01:00:00")
 
    ),
    autuacao as (
        select *
        from citran
        union all
        select *
        from serpro
    ),
    complete_partitions as (
        select *
        from autuacao
        
            union all
            select * except (id_autuacao, tipificacao_resumida, amparo_legal)
            from `rj-smtr`.`transito`.`autuacao`
            where data in ('2022-01-01', '2022-01-02', '2022-01-03', '2022-01-04', '2022-01-05', '2022-01-06', '2022-01-07', '2022-01-08', '2022-01-09', '2022-01-10', '2022-01-11', '2022-01-12', '2022-01-13', '2022-01-14', '2022-01-18', '2022-01-17', '2022-01-16', '2022-01-21', '2022-01-22', '2022-01-15', '2022-01-19', '2022-01-20', '2022-01-23', '2022-01-24', '2022-01-25', '2022-01-26', '2022-01-27', '2022-01-28', '2022-01-29', '2022-01-30', '2022-01-31', '2022-02-01', '2022-02-02', '2022-02-03', '2022-02-04', '2022-02-05', '2022-02-06', '2022-02-07', '2022-02-08', '2022-02-09', '2022-02-10', '2022-02-11', '2022-02-12', '2022-02-13', '2022-02-14', '2022-02-15', '2022-02-16', '2022-02-17', '2022-02-18', '2022-02-19', '2022-02-20', '2022-02-21', '2022-02-22', '2022-02-23', '2022-02-24', '2022-02-25', '2022-02-26', '2022-02-27', '2022-02-28', '2022-03-01', '2022-03-03', '2022-03-02', '2022-03-04', '2022-03-05', '2022-03-06', '2022-03-07', '2022-03-08', '2022-03-09', '2022-03-10', '2022-03-11', '2022-03-12', '2022-03-13', '2022-03-14', '2022-03-15', '2022-03-17', '2022-03-16', '2022-03-18', '2022-03-21', '2022-03-20', '2022-03-19', '2022-03-22', '2022-03-23', '2022-03-24', '2022-03-25', '2022-03-26', '2022-03-29', '2022-03-28', '2022-03-30', '2022-03-27', '2022-03-31', '2022-04-01', '2022-04-02', '2022-04-03', '2022-04-05', '2022-04-04', '2022-04-06', '2022-04-08', '2022-04-07', '2022-04-09', '2022-04-10', '2022-04-11', '2022-04-12', '2022-04-13', '2022-04-14', '2022-04-15', '2022-04-16', '2022-04-17', '2022-04-18', '2022-04-19', '2022-04-20', '2022-04-21', '2022-04-22', '2022-04-23', '2022-04-24', '2022-04-25', '2022-04-27', '2022-04-26', '2022-04-28', '2022-04-29', '2022-04-30', '2022-05-02', '2022-05-01', '2022-05-03', '2022-05-04', '2022-05-05', '2022-05-06', '2022-05-07', '2022-05-08', '2022-05-09', '2022-05-10', '2022-05-11', '2022-05-12', '2022-05-13', '2022-05-16', '2022-05-14', '2022-05-15', '2022-05-17', '2022-05-18', '2022-05-19', '2022-05-20', '2022-05-21', '2022-05-22', '2022-05-23', '2022-05-24', '2022-05-25', '2022-05-26', '2022-05-27', '2022-05-28', '2022-05-29', '2022-05-30', '2022-05-31', '2022-06-01', '2022-06-02', '2022-06-03', '2022-06-04', '2022-06-05', '2022-06-06', '2022-06-07', '2022-06-08', '2022-06-09', '2022-06-10', '2022-06-11', '2022-06-12', '2022-06-13', '2022-06-14', '2022-06-15', '2022-06-16', '2022-06-17', '2022-06-19', '2022-06-20', '2022-06-18', '2022-06-21', '2022-06-22', '2022-06-23', '2022-06-24', '2022-06-25', '2022-06-26', '2022-06-27', '2022-06-28', '2022-06-29', '2022-06-30', '2022-07-03', '2022-07-04', '2022-07-01', '2022-07-02', '2022-07-06', '2022-07-05', '2022-07-07', '2022-07-10', '2022-07-08', '2022-07-09', '2022-07-11', '2022-07-12', '2022-07-13', '2022-07-14', '2022-07-15', '2022-07-16', '2022-07-17', '2022-07-19', '2022-07-18', '2022-07-20', '2022-07-21', '2022-07-22', '2022-07-23', '2022-07-24', '2022-07-25', '2022-07-26', '2022-07-27', '2022-07-28', '2022-07-29', '2022-07-30', '2022-07-31', '2022-08-01', '2022-08-02', '2022-08-03', '2022-08-07', '2022-08-04', '2022-08-05', '2022-08-06', '2022-08-08', '2022-12-31', '2022-12-23', '2022-12-24', '2022-12-22', '2022-12-25', '2022-12-26', '2022-12-27', '2022-12-28', '2022-12-29', '2022-12-30')
        
    ),
    final as (
        select
            c.data,
            a.id_autuacao,
            c.id_auto_infracao,
            c.datetime_autuacao,
            c.data_limite_defesa_previa,
            c.data_limite_recurso,
            c.descricao_situacao_autuacao,
            c.status_infracao,
            c.codigo_enquadramento,
            i.tipificacao_resumida,
            c.pontuacao,
            c.gravidade,
            i.amparo_legal,
            c.tipo_veiculo,
            c.descricao_veiculo,
            c.placa_veiculo,
            c.ano_fabricacao_veiculo,
            c.ano_modelo_veiculo,
            c.cor_veiculo,
            c.especie_veiculo,
            c.uf_infrator,
            c.uf_principal_condutor,
            c.uf_proprietario,
            c.cep_proprietario,
            c.valor_infracao,
            c.valor_pago,
            c.data_pagamento,
            c.id_autuador,
            c.descricao_autuador,
            c.id_municipio_autuacao,
            c.descricao_municipio,
            c.uf_autuacao,
            c.endereco_autuacao,
            c.tile_autuacao,
            c.processo_defesa_autuacao,
            c.recurso_penalidade_multa,
            c.processo_troca_real_infrator,
            c.status_sne,
            c.fonte,
            c.datetime_ultima_atualizacao
        from complete_partitions as c
        left join autuacao_ids as a using (data, id_auto_infracao, fonte)
        left join infracoes_renainf as i using (codigo_enquadramento)
        qualify
            row_number() over (
                partition by a.id_autuacao order by c.datetime_ultima_atualizacao desc
            )
            = 1
    )
select *
from final