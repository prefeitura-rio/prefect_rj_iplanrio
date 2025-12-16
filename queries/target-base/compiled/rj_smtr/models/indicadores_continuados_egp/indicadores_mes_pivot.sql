


    
    
    
    
        
        
        
            
        
        
    
        
        
        
            
        
        
    
        
        
        
            
        
        
    
        
        
        
            
        
        
    
        
        
        
            
        
        
    
        
        
        
            
        
        
    

    




    
    
    SELECT
        "Idade média da frota operante por mês" AS indicador,
        "Ônibus" AS modo,
        *
    FROM (
        SELECT
            *
        FROM (
            SELECT
                ano,
                mes,
                valor
            FROM
                `rj-smtr`.`indicadores_continuados_egp`.`indicadores_mes`
                --rj-smtr.indicadores_continuados_egp.indicadores_mes
            WHERE
                nome_indicador = "Idade média da frota operante por mês"
                AND modo = "Ônibus"
        ) PIVOT ( MAX(valor) FOR mes IN (
            1 Janeiro,
            2 Fevereiro,
            3 Marco,
            4 Abril,
            5 Maio,
            6 Junho,
            7 Julho,
            8 Agosto,
            9 Setembro,
            10 Outubro,
            11 Novembro,
            12 Dezembro )
        )
    )
    UNION ALL
    

    
    
    SELECT
        "Passageiros pagantes por mês" AS indicador,
        "BRT" AS modo,
        *
    FROM (
        SELECT
            *
        FROM (
            SELECT
                ano,
                mes,
                valor
            FROM
                `rj-smtr`.`indicadores_continuados_egp`.`indicadores_mes`
                --rj-smtr.indicadores_continuados_egp.indicadores_mes
            WHERE
                nome_indicador = "Passageiros pagantes por mês"
                AND modo = "BRT"
        ) PIVOT ( MAX(valor) FOR mes IN (
            1 Janeiro,
            2 Fevereiro,
            3 Marco,
            4 Abril,
            5 Maio,
            6 Junho,
            7 Julho,
            8 Agosto,
            9 Setembro,
            10 Outubro,
            11 Novembro,
            12 Dezembro )
        )
    )
    UNION ALL
    
    
    SELECT
        "Passageiros pagantes por mês" AS indicador,
        "Ônibus" AS modo,
        *
    FROM (
        SELECT
            *
        FROM (
            SELECT
                ano,
                mes,
                valor
            FROM
                `rj-smtr`.`indicadores_continuados_egp`.`indicadores_mes`
                --rj-smtr.indicadores_continuados_egp.indicadores_mes
            WHERE
                nome_indicador = "Passageiros pagantes por mês"
                AND modo = "Ônibus"
        ) PIVOT ( MAX(valor) FOR mes IN (
            1 Janeiro,
            2 Fevereiro,
            3 Marco,
            4 Abril,
            5 Maio,
            6 Junho,
            7 Julho,
            8 Agosto,
            9 Setembro,
            10 Outubro,
            11 Novembro,
            12 Dezembro )
        )
    )
    UNION ALL
    

    
    
    SELECT
        "Gratuidades por mês" AS indicador,
        "BRT" AS modo,
        *
    FROM (
        SELECT
            *
        FROM (
            SELECT
                ano,
                mes,
                valor
            FROM
                `rj-smtr`.`indicadores_continuados_egp`.`indicadores_mes`
                --rj-smtr.indicadores_continuados_egp.indicadores_mes
            WHERE
                nome_indicador = "Gratuidades por mês"
                AND modo = "BRT"
        ) PIVOT ( MAX(valor) FOR mes IN (
            1 Janeiro,
            2 Fevereiro,
            3 Marco,
            4 Abril,
            5 Maio,
            6 Junho,
            7 Julho,
            8 Agosto,
            9 Setembro,
            10 Outubro,
            11 Novembro,
            12 Dezembro )
        )
    )
    UNION ALL
    
    
    SELECT
        "Gratuidades por mês" AS indicador,
        "Ônibus" AS modo,
        *
    FROM (
        SELECT
            *
        FROM (
            SELECT
                ano,
                mes,
                valor
            FROM
                `rj-smtr`.`indicadores_continuados_egp`.`indicadores_mes`
                --rj-smtr.indicadores_continuados_egp.indicadores_mes
            WHERE
                nome_indicador = "Gratuidades por mês"
                AND modo = "Ônibus"
        ) PIVOT ( MAX(valor) FOR mes IN (
            1 Janeiro,
            2 Fevereiro,
            3 Marco,
            4 Abril,
            5 Maio,
            6 Junho,
            7 Julho,
            8 Agosto,
            9 Setembro,
            10 Outubro,
            11 Novembro,
            12 Dezembro )
        )
    )
    UNION ALL
    

    
    
    SELECT
        "Frota operante por mês" AS indicador,
        "Ônibus" AS modo,
        *
    FROM (
        SELECT
            *
        FROM (
            SELECT
                ano,
                mes,
                valor
            FROM
                `rj-smtr`.`indicadores_continuados_egp`.`indicadores_mes`
                --rj-smtr.indicadores_continuados_egp.indicadores_mes
            WHERE
                nome_indicador = "Frota operante por mês"
                AND modo = "Ônibus"
        ) PIVOT ( MAX(valor) FOR mes IN (
            1 Janeiro,
            2 Fevereiro,
            3 Marco,
            4 Abril,
            5 Maio,
            6 Junho,
            7 Julho,
            8 Agosto,
            9 Setembro,
            10 Outubro,
            11 Novembro,
            12 Dezembro )
        )
    )
    
    
