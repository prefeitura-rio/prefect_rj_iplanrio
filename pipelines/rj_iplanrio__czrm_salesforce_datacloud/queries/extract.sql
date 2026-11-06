-- Extração de uma DLO do Data Cloud (conector Salesforce CRM).
--
-- $dlo    : nome da tabela no Data Cloud (ex.: WorkOrder_Home__dll)
-- $filtro : cláusula WHERE com a janela de SystemModstamp, ou vazio na carga inicial
--
-- SELECT * é intencional: a DLO tem 30 campos no WorkOrder e 105/106 no
-- Account, e o conjunto muda de org para org (prod tem um campo que a QA não
-- tem). A lista de colunas é descoberta em runtime por
-- tasks/extract.py:descobrir_colunas (SELECT * LIMIT 0) e usada para montar
-- o schema do BigQuery.
--
-- Sem ORDER BY/LIMIT: a paginação é aplicada por página em tasks/extract.py.
SELECT *
FROM $dlo
$filtro
