

with
    receita_fonte_unpivot as (
        select
            ano,
            case
                when mes = 'Janeiro'
                then '01'
                when mes = 'Fevereiro'
                then '02'
                when mes = 'Março'
                then '03'
                when mes = 'Abril'
                then '04'
                when mes = 'Maio'
                then '05'
                when mes = 'Junho'
                then '06'
                when mes = 'Julho'
                then '07'
                when mes = 'Agosto'
                then '08'
                when mes = 'Setembro'
                then '09'
                when mes = 'Outubro'
                then '10'
                when mes = 'Novembro'
                then '11'
                when mes = 'Dezembro'
                then '12'
            end as mes,
            safe_cast(
                replace(replace(valor_arrecadacao, '.', ''), ',', '.') as numeric
            ) as valor_arrecadacao,
            fonte
        from
            `rj-smtr-staging`.`transito_staging`.`receita_autuacao_fonte` unpivot (
                valor_arrecadacao for mes in (
                    `Janeiro`,
                    `Fevereiro`,
                    `Março`,
                    `Abril`,
                    `Maio`,
                    `Junho`,
                    `Julho`,
                    `Agosto`,
                    `Setembro`,
                    `Outubro`,
                    `Novembro`,
                    `Dezembro`
                )
            )
    ),

    receita_com_data as (
        select
            parse_date('%Y-%m-%d', concat(ano, '-', mes, '-01')) as data,
            ano,
            mes,
            valor_arrecadacao,
            fonte
        from receita_fonte_unpivot
        where valor_arrecadacao is not null
    )

select data, ano, mes, valor_arrecadacao, fonte
from receita_com_data

    where
        data between date("2022-01-01T00:00:00") and date(
            "2022-01-01T01:00:00"
        )
