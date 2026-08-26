-- Overpayment / underpayment check: sums valor_pago per NF and compares
-- against the declared valor_documento.
-- $pdf_filter is an optional WHERE clause built in Python (empty string
-- when no PDF-name filter is requested).
SELECT
    descricao_limpa AS pdf_name,
    cnpj,
    num_documento AS numero_nf,
    valor_documento,
    SUM(valor_pago) AS valor_pago_total,
    ROUND(SUM(valor_pago) - valor_documento, 2) AS difference,
    CASE
        WHEN SUM(valor_pago) > valor_documento THEN 'OVERPAID'
        WHEN SUM(valor_pago) = valor_documento THEN 'OK'
        WHEN SUM(valor_pago) < valor_documento THEN 'UNDERPAID'
        ELSE 'UNKNOWN'
    END AS status
FROM
    `$project.$dataset.osinfo_despesas_recorte`
$pdf_filter
GROUP BY
    descricao_limpa,
    cnpj,
    num_documento,
    valor_documento
HAVING
    SUM(valor_pago) != valor_documento
ORDER BY
    difference DESC
