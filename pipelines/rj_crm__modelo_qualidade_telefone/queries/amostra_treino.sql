-- data_corte sorteada por telefone (hash determinístico, não numpy — ver README do
-- repo do modelo), uniforme entre o 1º disparo e `max − janela_futuro_dias`: o
-- rótulo precisa de `janela_futuro_dias` de futuro depois do corte.
periodo AS (
  SELECT
    MIN(envio_datahora) AS inicio_dataset,
    TIMESTAMP_SUB(MAX(envio_datahora), INTERVAL $janela_futuro_dias DAY) AS fim_sorteio
  FROM disparos
),
amostra AS (
  -- data_corte sorteada por telefone, uniforme em [inicio_dataset, fim_sorteio]
  SELECT
    u.telefone,
    TIMESTAMP_ADD(
      p.inicio_dataset,
      INTERVAL MOD(
        ABS(FARM_FINGERPRINT(CONCAT(u.telefone, '_', CAST($random_state AS STRING)))),
        DATE_DIFF(DATE(p.fim_sorteio), DATE(p.inicio_dataset), DAY) + 1
      ) DAY
    ) AS data_corte
  FROM universo u
  CROSS JOIN periodo p
),
