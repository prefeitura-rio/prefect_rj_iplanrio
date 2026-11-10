-- recado_servidores_pref_rio_14

WITH 

dados_rmi AS (
    -- 1. TELEFONE PRINCIPAL (STRUCT simples - acesso direto por ponto)
    SELECT
        LPAD(CAST(rmi.cpf AS STRING), 11, '0') AS cpf,
        rmi.telefone.principal.ddd AS ddd_rmi,
        REGEXP_REPLACE(rmi.telefone.principal.valor, r'\D', '') AS tel_completo_rmi,
        RIGHT(REGEXP_REPLACE(rmi.telefone.principal.valor, r'\D', ''), 9) AS tel_9_digitos_rmi
    FROM `rj-crm-registry.rmi_dados_mestres.pessoa_fisica` AS rmi
    WHERE rmi.menor_idade IS FALSE
      AND rmi.obito.indicador IS FALSE
      AND rmi.telefone.principal.estrategia_envio IN ('ENVIAR', 'TESTAR')
      AND rmi.telefone.principal.valor IS NOT NULL

    UNION ALL

    -- 2. TELEFONE ALTERNATIVO (ARRAY DE STRUCTS - precisa de UNNEST)
    SELECT
        LPAD(CAST(rmi.cpf AS STRING), 11, '0') AS cpf,
        t.ddd AS ddd_rmi,
        REGEXP_REPLACE(t.valor, r'\D', '') AS tel_completo_rmi,
        RIGHT(REGEXP_REPLACE(t.valor, r'\D', ''), 9) AS tel_9_digitos_rmi
    FROM `rj-crm-registry.rmi_dados_mestres.pessoa_fisica` AS rmi
    LEFT JOIN UNNEST(rmi.telefone.alternativo) AS t
    WHERE rmi.menor_idade IS FALSE
      AND rmi.obito.indicador IS FALSE
      AND t.estrategia_envio IN ('ENVIAR', 'TESTAR')
      AND t.valor IS NOT NULL
),

rmi_agrupado AS (
    -- Agrupa os telefones do RMI por CPF para cruzamento
    SELECT
        cpf,
        ARRAY_AGG(STRUCT(tel_9_digitos_rmi AS num_9, ddd_rmi AS ddd)) AS rmi_tels
    FROM dados_rmi
    GROUP BY 1
),

servidores_base AS (
    SELECT 
        INITCAP(
            IF(
                ARRAY_LENGTH(SPLIT(NOME, ' ')) > 1,
                CONCAT(
                    SPLIT(NOME, ' ')[SAFE_OFFSET(0)], ' ',
                    SPLIT(NOME, ' ')[SAFE_OFFSET(ARRAY_LENGTH(SPLIT(NOME, ' ')) - 1)]
                ),
                NOME
            )
        ) AS nome_servidor,
        LPAD(CAST(CPF AS STRING), 11, '0') AS cpf,
        -- Separa os números quebrados por '/' e limpa caracteres não numéricos
        [
            REGEXP_REPLACE(SPLIT(celular, '/')[SAFE_OFFSET(0)], r'\D', ''),
            REGEXP_REPLACE(SPLIT(celular, '/')[SAFE_OFFSET(1)], r'\D', ''),
            REGEXP_REPLACE(SPLIT(TELEFONE, '/')[SAFE_OFFSET(0)], r'\D', ''),
            REGEXP_REPLACE(SPLIT(TELEFONE, '/')[SAFE_OFFSET(1)], r'\D', '')
        ] AS candidatos_raw
    FROM `rj-crm-registry-dev.teste.bonus_servidor_segundo_envio`
),

candidatos_processados AS (
    SELECT
        s.nome_servidor,
        s.cpf,
        cand AS tel_raw,
        idx,
        -- Busca DDD no RMI para números de 9 dígitos; se não achar, aplica '21' como fallback
        CASE 
            WHEN LENGTH(cand) = 9 THEN COALESCE(
                (
                    SELECT CONCAT(r.ddd, cand)
                    FROM UNNEST(rmi.rmi_tels) AS r
                    WHERE r.num_9 = cand
                    LIMIT 1
                ),
                CONCAT('21', cand)
            )
            ELSE cand
        END AS tel_com_ddd
    FROM servidores_base s
    LEFT JOIN rmi_agrupado rmi ON s.cpf = rmi.cpf
    CROSS JOIN UNNEST(s.candidatos_raw) AS cand WITH OFFSET idx
    WHERE cand IS NOT NULL AND cand != ''
),

candidatos_validados AS (
    SELECT
        cpf,
        nome_servidor,
        idx,
        `rj-crm-registry.udf.VALIDATE_AND_FORMAT_PHONE`(tel_com_ddd) AS telefone_validado
    FROM candidatos_processados
),

dados_final AS (
    SELECT
        nome_servidor,
        cpf,
        -- Respeita a ordem de prioridade original (celular[0] > celular[1] > telefone[0] > telefone[1])
        ARRAY_AGG(telefone_validado IGNORE NULLS ORDER BY idx LIMIT 1)[SAFE_OFFSET(0)] AS telefone
    FROM candidatos_validados
    GROUP BY nome_servidor, cpf
)

SELECT * 
FROM dados_final 
WHERE cpf IS NOT NULL 
  AND telefone IS NOT NULL
union all
select 'Edy', '00000000354', '5511974243228'
;