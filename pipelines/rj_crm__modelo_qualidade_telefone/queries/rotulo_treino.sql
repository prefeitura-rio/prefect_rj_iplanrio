-- ---- 1) rótulo: HighDelivery na janela futura --------------------------
futuro AS (
  SELECT telefone, status_disparo IN ('delivered', 'read') AS entregue
  FROM disparo_amostra
  WHERE envio_datahora >= data_corte
    AND envio_datahora < TIMESTAMP_ADD(data_corte, INTERVAL $janela_futuro_dias DAY)
),
rotulo AS (
  SELECT
    telefone,
    IF(SAFE_DIVIDE(COUNTIF(entregue), COUNT(*)) > $limiar_high_delivery, 1, 0) AS high_delivery
  FROM futuro
  GROUP BY telefone
),
