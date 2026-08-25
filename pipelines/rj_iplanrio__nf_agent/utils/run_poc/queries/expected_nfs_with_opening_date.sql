-- Expected NFs from despesas_recorte, joined with bcadastro for the
-- company's opening date (used for service-date validation).
-- $pdf_filter is an optional WHERE clause built in Python (empty string
-- when no PDF-name filter is requested).
WITH despesas AS (
    SELECT
        id_documento,
        descricao_limpa,
        cnpj,
        num_documento,
        valor_documento,
        data_envio,
        data_emissao,
        cod_organizacao,
        cod_unidade,
        valor_pago,
        LPAD(REGEXP_REPLACE(cnpj, r'[^0-9]', ''), 14, '0') AS cleaned_cnpj
    FROM
        `$project.$dataset.osinfo_despesas_recorte`
),
bcadastro AS (
    SELECT
        LPAD(REGEXP_REPLACE(CAST(cnpj_particao AS STRING), r'[^0-9]', ''), 14, '0') AS cleaned_cnpj_reg,
        inicio_atividade_data AS cnpj_data_abertura
    FROM
        `$project.$dataset.bcadastro_cnpj_recorte`
)
SELECT
    d.id_documento,
    d.descricao_limpa AS pdf_name,
    d.cnpj,
    d.num_documento AS numero_nf,
    d.valor_documento AS valor_total,
    d.data_envio,
    d.data_emissao,
    d.cod_organizacao,
    d.cod_unidade,
    d.valor_pago,
    b.cnpj_data_abertura,
    SUM(d.valor_pago) AS valor_pago_total,
    COUNT(*) AS num_parcelas
FROM
    despesas d
LEFT JOIN
    bcadastro b ON d.cleaned_cnpj = b.cleaned_cnpj_reg
$pdf_filter
GROUP BY
    d.id_documento,
    d.descricao_limpa,
    d.cnpj,
    d.num_documento,
    d.valor_documento,
    d.data_envio,
    d.data_emissao,
    d.cod_organizacao,
    d.cod_unidade,
    d.valor_pago,
    b.cnpj_data_abertura
ORDER BY
    d.data_envio,
    d.id_documento
