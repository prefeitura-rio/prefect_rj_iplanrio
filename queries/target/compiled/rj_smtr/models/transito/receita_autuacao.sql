

with
    receita_agregada as (
        select data, ano, mes, sum(valor_arrecadacao) as valor_arrecadacao
        from `rj-smtr`.`transito`.`receita_autuacao_fonte`
        group by data, ano, mes
    )

select data, ano, mes, valor_arrecadacao
from receita_agregada

    where
        data between date("2022-01-01T00:00:00") and date(
            "2022-01-01T01:00:00"
        )
