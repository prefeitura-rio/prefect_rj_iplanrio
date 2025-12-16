

with
    licenciamento as (
        select distinct date(data) as data_licenciamento
        from `rj-smtr`.`cadastro_staging`.`licenciamento_stu`
        
            where
                date(data) between date("2022-01-01T01:00:00") and date(
                    "2022-01-08 01:00:00"
                )
        
    ),
    periodo as (
        select data
        from
            unnest(
                -- Primeira data de captura de licenciamento
                generate_date_array('2022-03-21', current_date("America/Sao_Paulo"))
            ) as data
        
            where data between "2022-01-01T01:00:00" and "2022-01-01T01:00:00"
        
    ),
    data_versao_calc as (
        select
            periodo.data,
            (
                select
                    case
                        /* Versão fixa do STU em 2024-03-25 para mar/Q1 devido à falha de
             atualização na fonte da dados (SIURB) */
                        when
                            date(periodo.data) >= date("2024-03-01")
                            and date(periodo.data) < "2024-03-16"
                        then date("2024-03-25")
                        /* Versão fixa do STU em 2024-04-09 para mar/Q2 devido à falha de
             atualização na fonte da dados (SIURB) */
                        when
                            date(periodo.data) >= date("2024-03-16")
                            and date(periodo.data) < "2024-04-01"
                        then date("2024-04-09")
                        else
                            (
                                select min(data_licenciamento)
                                from licenciamento
                                where
                                    data_licenciamento
                                    >= date_add(date(periodo.data), interval 5 day)
                                    /* Admite apenas versões do STU igual ou após 2024-04-09 a
                         partir de abril/24 devido à falha de atualização na fonte
                         de dados (SIURB) */
                                    and (
                                        date(periodo.data) < "2024-04-01"
                                        or data_licenciamento >= '2024-04-09'
                                    )
                            )
                    end
            ) as data_versao
        from periodo
    )
select *
from data_versao_calc
where data_versao is not null