-- Maior NSU já processado e carregado, de qualquer versão do parser: uma versão nova continua de onde a anterior parou
-- (só a quarentena é reavaliada; reprocessar o histórico com regras novas é uma operação à parte).
SELECT COALESCE(MAX(ultimo_nsu), 0) AS ultimo_nsu
FROM `$project.$dataset.$marca`
