-- Disparo para todos as pessoas válidas (maiores de 18 e vivas), na cidade do Rio com telefones celulares relativamente bons
with final as (
  SELECT
    cpf,
    COALESCE(if(nome_social = "", null, nome_social), nome) AS nome,
    `rj-crm-registry.udf.VALIDATE_AND_FORMAT_PHONE`(
    CONCAT(IFNULL(telefone.principal.ddi, '55'), telefone.principal.ddd, telefone.principal.valor)
    ) AS telefone,
    CASE
    WHEN pf.telefone.principal.estrategia_envio = "ENVIAR" THEN 0
    WHEN pf.telefone.principal.estrategia_envio = "TESTAR" THEN 1
    WHEN pf.telefone.principal.estrategia_envio = "EVITAR" THEN 2
    ELSE 3
    END AS phone_priority,
    pf.telefone.principal.estrategia_envio,
    pf.telefone.principal.qualidade
FROM `rj-crm-registry.rmi_dados_mestres.pessoa_fisica` pf
WHERE pf.telefone.principal.qualidade = 'VALIDO'
    AND pf.telefone.principal.estrategia_envio != "NÃO ENVIAR"
    AND pf.telefone.principal.estrategia_envio in ("ENVIAR", "TESTAR")--, "EVITAR")
    AND pf.telefone.principal.tipo = "CELULAR"
    AND pf.telefone.principal.valor IS NOT NULL
    AND pf.menor_idade IS FALSE
    AND pf.obito.indicador IS FALSE
    AND pf.endereco.principal.municipio = "Rio de Janeiro"
)

select 
    cpf as SubscriberKey,
    CONCAT(
        INITCAP(SPLIT(nome, ' ')[OFFSET(0)]),
        ' ',
        INITCAP(SPLIT(nome, ' ')[OFFSET(ARRAY_LENGTH(SPLIT(nome,' ')) - 1)])
    ) as nome,
  telefone,
  "BR" as Locale
from final
where telefone is not null
order by phone_priority