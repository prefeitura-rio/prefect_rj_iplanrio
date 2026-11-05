# Cenários de Teste para Fluxo de Retentativas (HSM Repescagem)

Este documento contém queries SQL para simular diferentes cenários de falha e sucesso no sistema de retentativas do CRM.

---

## Configuração Comum
Para todos os testes, considere os seguintes parâmetros no Prefect:
- `max_dispatch_retries`: Define o limite de tentativas extras.
- `filter_duplicated_phones`: Deve estar como `True` para validar a desduplicação entre diferentes telefones em cada retry.
- `filter_duplicated_cpfs`: Deve estar como `True` para validar a desduplicação entre diferentes CPFs em cada retry.
- `filter_dispatched_phones_or_cpfs`: Testar com `phone` e `cpf`. Se for `cpf` o código deverá trocar nos retries para None, porque já haverá sido feito um dispatch para aqueles cpfs.

---

## Teste 1: Correspondência Exata de Retentativas
**Objetivo:** Validar que o fluxo percorre toda a lista de `others` até encontrar um número válido.

- **Configuração:**
    - `max_dispatch_retries = 2`
    - `test_mode = True` para não recebermos o comunicado no discord e nem salvar o dado no data lake
    - `filter_dispatched_phones_or_cpfs`: Testar com `phone` ou `cpf`.

- **Cenário:** 
    1. `celular_disparo`: Falha (Número inexistente).
    2. `others[0]`: Falha (Número inválido).
    3. `others[1]`: **Sucesso** (Número real).
- **Expectativa:** O flow deve realizar 3 ciclos de disparo para este externalId.

```sql
SELECT
    TO_JSON_STRING(
        STRUCT(
            '5521971111111' AS celular_disparo, -- 1ª Tentativa: Falha proposital
            CAST(101 AS STRING) AS externalId,
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
            [
                '5521981111111', -- 2ª Tentativa (Repescagem 1): Falha proposital
                '5511984677798'  -- 3ª Tentativa (Repescagem 2): SUCESSO ESPERADO
            ] AS others
        )
    ) AS destination_data
```

---

## Teste 2: Exaustão de Números e Campos Ausentes/Nulos
**Objetivo:** Validar que o código não quebra quando pedimos mais retentativas do que a pessoa possui de números extras ou números incompletos na lista de retentativa.

- **Configuração:** 
    - `max_dispatch_retries = 2`
    - `test_mode = True` para não recebermos o comunicado no discord e nem salvar o dado no data lake
    - `filter_dispatched_phones_or_cpfs`: Testar com `phone` ou `cpf`.

- **Cenários:** 
    - **ID 201:** Possui apenas 1 número extra (Exaustão na 3ª tentativa).
    - **ID 202:** Possui lista `others` vazia `[]`.
    - **ID 203:** Não possui o campo `others` no JSON.

```sql
-- 201: Exaustão
SELECT
    TO_JSON_STRING(
        STRUCT(
            '5521980000000' AS celular_disparo, -- 1ª Tentativa: Falha
            '201' AS externalId,
            STRUCT(
                'Teste Exaustão' AS NOME_SOBRENOME,
                'Teste Exaustão' AS CC_WT_NOME_SOBRENOME,
                'Iluminação Pública' AS SOLICITACAO,
                'Iluminação Pública' AS CC_WT_SOLICITACAO,
                'App 1746' AS CANAL,
                'App 1746' AS CC_WT_CANAL,
                '12345678901' AS CC_WT_CPF_CIDADAO,
                '20260309001' AS CC_WT_ID_CHAMADO,
                '42' AS CC_WT_ID_SUBTIPO,
                'ABERTO' AS CC_WT_STATUS_CHAMADO
            ) AS vars,
            [
                '5521961111111' -- 2ª Tentativa: Falha. Não há 3ª tentativa disponível.
            ] AS others
        )
    ) AS destination_data

UNION ALL

-- 202: Lista Vazia
SELECT
    TO_JSON_STRING(
        STRUCT(
            '5521970000000' AS celular_disparo,
            '202' AS externalId,
            STRUCT(
                'Teste Vazio' AS NOME_SOBRENOME,
                'Teste Vazio' AS CC_WT_NOME_SOBRENOME,
                'Iluminação Pública' AS SOLICITACAO,
                'Iluminação Pública' AS CC_WT_SOLICITACAO,
                'App 1746' AS CANAL,
                'App 1746' AS CC_WT_CANAL,
                '12345678901' AS CC_WT_CPF_CIDADAO,
                '20260309001' AS CC_WT_ID_CHAMADO,
                '42' AS CC_WT_ID_SUBTIPO,
                'ABERTO' AS CC_WT_STATUS_CHAMADO
            ) AS vars,
            [] AS others
        )
    ) AS destination_data

UNION ALL

-- 203: Campo Omitido
SELECT
    TO_JSON_STRING(
        STRUCT(
            '5521990000000' AS celular_disparo,
            '203' AS externalId,
            STRUCT(
                'Teste Omitido' AS NOME_SOBRENOME,
                'Teste Omitido' AS CC_WT_NOME_SOBRENOME,
                'Iluminação Pública' AS SOLICITACAO,
                'Iluminação Pública' AS CC_WT_SOLICITACAO,
                'App 1746' AS CANAL,
                'App 1746' AS CC_WT_CANAL,
                '12345678901' AS CC_WT_CPF_CIDADAO,
                '20260309001' AS CC_WT_ID_CHAMADO,
                '42' AS CC_WT_ID_SUBTIPO,
                'ABERTO' AS CC_WT_STATUS_CHAMADO
            ) AS vars
        )
    ) AS destination_data
```

---

## Teste 3: Concorrência e Desduplicação (Mesmo Telefone em CPFs Diferentes dentro da retentativa)

**Objetivo:** Validar se a função `remove_duplicate_phones` funciona corretamente quando o próximo número da fila de uma pessoa já foi usado por outra pessoa no mesmo ciclo (retry).

- **Configuração:** 
    - `max_dispatch_retries = 2`
    - `test_mode = True` para não recebermos o comunicado no discord e nem salvar o dado no data lake
    - `filter_dispatched_phones_or_cpfs`: Testar com `phone` ou `cpf`.

- **Cenário:**
    - CPF 301 e CPF 302 falham no primeiro número.
    - Ambos possuem o mesmo número `'5521981111111'` no `others[0]` como primeira opção de repescagem e o disparo será feito apenas para o 301.
    - Na segunda retentativa o CPF 302 terá um disparo para o `others[1]`.

- **Expectativa:** No ciclo de Retentativa 1, apenas o CPF 301 deve receber o disparo. O segundo deve ser filtrado para evitar spam/bloqueio da API. E na Retentativa 2, o CPF 302 deve receber o disparo.

```sql
-- CPF 301: Vai tentar o final 11111 na retentativa 1
SELECT
    TO_JSON_STRING(
        STRUCT(
            '5521960000000' AS celular_disparo,
            '301' AS externalId,
            STRUCT(
                'Teste Duplicado A' AS NOME_SOBRENOME,
                'Teste Duplicado A' AS CC_WT_NOME_SOBRENOME,
                'Iluminação Pública' AS SOLICITACAO,
                'Iluminação Pública' AS CC_WT_SOLICITACAO,
                'App 1746' AS CANAL,
                'App 1746' AS CC_WT_CANAL,
                '12345678901' AS CC_WT_CPF_CIDADAO,
                '20260309001' AS CC_WT_ID_CHAMADO,
                '42' AS CC_WT_ID_SUBTIPO,
                'ABERTO' AS CC_WT_STATUS_CHAMADO
            ) AS vars,
            [
            '5521981111111' -- ERRO: 2ª Tentativa / 1ª Repescagem (Número inválido)
            ] AS others
        )
    ) AS destination_data

UNION ALL

-- CPF 302: Também possui o final 11111 na retentativa 1. Deve ser desduplicado.
SELECT
    TO_JSON_STRING(
        STRUCT(
            '5521981000000' AS celular_disparo,
            '302' AS externalId,
            STRUCT(
                'Teste Duplicado B' AS NOME_SOBRENOME,
                'Teste Duplicado B' AS CC_WT_NOME_SOBRENOME,
                'Iluminação Pública' AS SOLICITACAO,
                'Iluminação Pública' AS CC_WT_SOLICITACAO,
                'App 1746' AS CANAL,
                'App 1746' AS CC_WT_CANAL,
                '12345678901' AS CC_WT_CPF_CIDADAO,
                '20260309001' AS CC_WT_ID_CHAMADO,
                '42' AS CC_WT_ID_SUBTIPO,
                'ABERTO' AS CC_WT_STATUS_CHAMADO
            ) AS vars,
            [
            '5521981111111', -- ERRO: 2ª Tentativa / 1ª Repescagem (Número inválido)
            '5521991111111' -- ERRO: 3ª Tentativa / 2ª Repescagem (Número inválido)
            ] AS others
        )
    ) AS destination_data
```

---

## Teste 4: Variações de Case (Chaves em Maiúsculo)

**Objetivo:** Validar que o Python encontra os campos mesmo se as chaves do BigQuery vierem em caixa alta.

- **Configuração:** 
    - `max_dispatch_retries = 2`
    - `test_mode = True` para não recebermos o comunicado no discord e nem salvar o dado no data lake
    - `filter_dispatched_phones_or_cpfs`: Testar com `phone` ou `cpf`.

- **Cenários:** 
    - **ID 401:** Possui apenas 2 números extras, mas nome dos campos vêm em caixa alta

- **Expectativa:** Disparo deve ser feito para as duas tentativas.

```sql
SELECT
    TO_JSON_STRING(
        STRUCT(
            '5521918000000' AS CELULAR_DISPARO, -- ERRO: 1ª Tentativa (Número inexistente)
            '401' AS EXTERNALID, -- Chave em maiúsculo
            STRUCT(
                'Teste Case' AS NOME_SOBRENOME,
                'Teste Case' AS CC_WT_NOME_SOBRENOME,
                'Iluminação Pública' AS SOLICITACAO,
                'Iluminação Pública' AS CC_WT_SOLICITACAO,
                'App 1746' AS CANAL,
                'App 1746' AS CC_WT_CANAL,
                '12345678901' AS CC_WT_CPF_CIDADAO,
                '20260309001' AS CC_WT_ID_CHAMADO,
                '42' AS CC_WT_ID_SUBTIPO,
                'ABERTO' AS CC_WT_STATUS_CHAMADO
            ) AS VARS,
            [
            '5521928111111' -- ERRO: 2ª Tentativa / 1ª Repescagem (Número inválido)
            ] AS OTHERS
        )
    ) AS destination_data
```


# Query para verificar se os cpfs estão aparecendo com status failed no DL
```sql
WITH status_por_disparo AS (
    SELECT
        targetExternalId,
        createDate,
        MAX(
            CASE
                WHEN status = "PROCESSING" THEN 1
                WHEN status = "FAILED" THEN 2
                WHEN status = "SENT" THEN 3
                WHEN status = "DELIVERED" THEN 4
                WHEN status = "READ" THEN 5
            END
        ) AS id_status_disparo
    FROM `rj-crm-registry.brutos_wetalkie_staging.fluxo_atendimento_*`
    WHERE templateId = 192
      AND sendDate >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
    GROUP BY targetExternalId, createDate
),

ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY targetExternalId
            ORDER BY createDate DESC
        ) AS rn
    FROM status_por_disparo
)

SELECT targetExternalId
FROM ranked
WHERE rn = 1
  AND id_status_disparo = 2
order by 1
  -- rn = 1 para pegar o último disparo e id_status_disparo = 2 para selecionar apenas os com falha
```

## Query para verificar últimos disparos
```sql
select * FROM `rj-crm-registry.brutos_wetalkie_staging.fluxo_atendimento_*`
    WHERE templateId = 192
      AND sendDate >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
      order by targetExternalId,
        createDate, datarelay_timestamp
```