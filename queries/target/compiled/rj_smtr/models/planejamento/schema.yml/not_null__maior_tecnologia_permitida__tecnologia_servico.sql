with
        servicos_validos as (
            select distinct servico
            from `rj-smtr`.`projeto_subsidio_sppo`.`viagem_planejada`
            where
                data
                between '2022-01-01T00:00:00'
                and '2022-01-01T01:00:00'
        )

    select *
    from 
    
        
        

        

        
        (select * from `rj-smtr`.`planejamento`.`tecnologia_servico` where inicio_vigencia <= date('2022-01-01T01:00:00') and (fim_vigencia is null OR fim_vigencia >= date('2022-01-01T00:00:00')))
    where
        maior_tecnologia_permitida is null and servico in (select servico from servicos_validos)
