-- Query to test retry flow with 2 retries.
-- parameter max_dispatch_retries should be set to 2
-- set max_dispatch_retries to 3 to see how the flow behaves when just 2 extra cellphones are provided in the others array (the last retry would have no candidates to choose from and should be skipped)
SELECT
         TO_JSON_STRING(
             STRUCT(
                 '5521000000000' AS celular_disparo, -- ERRO: 1ª Tentativa (Número inexistente)
                 CAST(1 AS STRING) AS externalId,
                 STRUCT(
                     'Teste Repescagem' AS NOME_SOBRENOME,
                     'Teste Repescagem' AS CC_WT_NOME_SOBRENOME,
                     'Iluminação Pública' AS SOLICITACAO,
                     'Iluminação Pública' AS CC_WT_SOLICITACAO,
                     'App 1746' AS CANAL,
                     'App 1746' AS CC_WT_CANAL,
                     '12345678901' AS CC_WT_CPF_CIDADAO,
                     '20260309001' AS CC_WT_ID_CHAMADO,
                     '42' AS CC_WT_ID_SUBTIPO,
                     'ABERTO' AS CC_WT_STATUS_CHAMADO
                 ) AS vars,
                 -- Array de repescagem que o Python usará:
                 [
                   '5521111111111', -- ERRO: 2ª Tentativa / 1ª Repescagem (Número inválido)
                   '5511984677798'  -- SUCESSO: 3ª Tentativa / 2ª Repescagem (Número válido)
                 ] AS others
             )
         ) AS destination_data