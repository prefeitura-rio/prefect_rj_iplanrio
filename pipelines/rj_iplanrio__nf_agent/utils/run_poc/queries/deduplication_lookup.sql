-- All unique (cnpj, num_documento, org, unit, pdf_name, data_envio, id)
-- combinations, used to build the in-memory deduplication lookup.
SELECT DISTINCT
    id_documento,
    cnpj,
    num_documento,
    descricao_limpa AS pdf_name,
    data_envio,
    cod_organizacao,
    cod_unidade
FROM
    `$project.$dataset.osinfo_despesas_recorte`
WHERE
    cnpj IS NOT NULL
    AND num_documento IS NOT NULL
ORDER BY
    cnpj, num_documento, cod_organizacao, cod_unidade, data_envio, id_documento
