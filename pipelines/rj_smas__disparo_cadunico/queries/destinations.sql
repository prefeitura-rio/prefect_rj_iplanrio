        WITH
        source_ AS (
        SELECT
            `rj-crm-registry.udf.VALIDATE_AND_FORMAT_CELLPHONE`(REGEXP_REPLACE(telefone, r'[^\d]', '')) AS celular_disparo,
            INITCAP(
              IF(
                ARRAY_LENGTH(SPLIT(nome_completo, ' ')) > 1,
                CONCAT(
                  SPLIT(nome_completo, ' ')[SAFE_OFFSET(0)],
                  ' ',
                  SPLIT(nome_completo, ' ')[SAFE_OFFSET(ARRAY_LENGTH(SPLIT(nome_completo, ' ')) - 1)]
                ),
                nome_completo
              )
            ) AS nome_sobrenome,
            id,
            FORMAT_DATETIME('%d/%m/%Y', DATETIME(data_hora)) AS DATA,
            FORMAT_DATETIME('%H:%M', DATETIME(data_hora)) AS HORA,
            unidade_nome,
            CONCAT(unidade_endereco, ' - ', unidade_bairro) as unidade_endereco,
            cpf
        FROM `rj-iplanrio.brutos_data_metrica_staging.cadunico_agendamentos`
        WHERE
            DATE(data_hora) = DATE_ADD( CURRENT_DATE("America/Sao_Paulo"), INTERVAL CAST({days_ahead_placeholder} AS int64) DAY) ),
        celulares_disparados AS (
        SELECT
            `rj-crm-registry.udf.VALIDATE_AND_FORMAT_CELLPHONE`(tel.contato_telefone) AS contato_telefone
        FROM `rj-crm-registry.crm_whatsapp.telefone_disparado` tel
        WHERE
            id_hsm = cast({id_hsm_placeholder} as string)
            AND data_particao >= DATE_SUB(CURRENT_DATE(), INTERVAL 15 DAY) ),
        filtra_celulares_disparados AS (
        SELECT
            source_.*
        FROM source_
        LEFT JOIN celulares_disparados
        ON source_.celular_disparo = contato_telefone
        WHERE
            celulares_disparados.contato_telefone IS NULL ),
        filtra_celulares_sem_whats AS (
        SELECT
            DISTINCT s.*
        FROM filtra_celulares_disparados AS s
        LEFT JOIN `rj-crm-registry.intermediario_rmi_telefones.int_telefone` AS tel
        ON s.celular_disparo = tel.telefone_numero_completo
        LEFT JOIN
            UNNEST(tel.consentimento) AS c
        WHERE
            (c.indicador_quarentena = FALSE
            AND tel.telefone_qualidade != "INVALIDO")
            OR tel.telefone_numero_completo IS NULL )
        SELECT
        TO_JSON_STRING(
          STRUCT(
            celular_disparo,
            STRUCT( nome_sobrenome AS NOME_SOBRENOME,
                DATA,
                HORA,
                unidade_nome AS LOCAL,
                unidade_endereco AS ENDERECO,
                cpf as CC_WT_CPF_CIDADAO,
                nome_sobrenome as CC_WT_NOME_SOBRENOME,
                unidade_nome as CC_WT_LOCAL,
                unidade_endereco as CC_WT_ENDERECO,
                DATA as CC_WT_DATA,
                HORA AS CC_WT_HORA,
                id as CC_WT_ID_AGENDAMENTO
            ) AS vars,
            cpf AS externalId )
        ) AS destination_data
        FROM filtra_celulares_sem_whats
    
