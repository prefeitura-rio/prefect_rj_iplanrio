with chamado as (
  SELECT  
    distinct
    chamado.id_logradouro,
    logradouro.tipo_logradouro as tipo_logradouro,
    logradouro.nome_completo as logradouro,
    chamado.numero_logradouro,
    chamado.id_bairro,
    bairro.nome as bairro,
    regexp_replace(CONCAT(
        CASE WHEN logradouro.nome_completo IS NULL THEN '' ELSE CONCAT(logradouro.nome_completo, " ") END,
        CASE WHEN chamado.numero_logradouro IS NULL THEN '' ELSE cast(chamado.numero_logradouro as string) END,
        ', ',
        CASE WHEN bairro.nome IS NULL THEN "Rio de Janeiro, RJ" ELSE concat(bairro.nome, ", Rio de Janeiro, RJ, Brasil") END
    ), r'\s+,', ',') AS endereco_completo 

    FROM `rj-segovi.adm_central_atendimento_1746.chamado` as chamado
    left join `rj-escritorio.dados_mestres.logradouro` as logradouro using(id_logradouro)
    left join `rj-escritorio.dados_mestres.bairro` as bairro on chamado.id_bairro = bairro.id_bairro

    WHERE latitude is null and id_logradouro is not null
)

select chamado.* from chamado
left join `rj-crm-registry.brutos_dados_enriquecidos_staging.enderecos_geolocalizados_chamado` geo using(endereco_completo)
where geo.endereco_completo is null
ORDER BY RAND()
    limit 50000
