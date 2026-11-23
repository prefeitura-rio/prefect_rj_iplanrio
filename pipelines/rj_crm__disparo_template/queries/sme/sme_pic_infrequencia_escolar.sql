    DECLARE semente STRING DEFAULT '170493';

    CREATE TEMP TABLE elegiveis AS
    WITH
        base AS (
            SELECT *
            FROM `rj-pic-dev.piloto_meier_iraja.ei_frequencia_geo_2026`
            WHERE grupamento IN ('Pré-escola I', 'Pré-escola II')
                AND baixa_freq_ambos
                AND em_ra_piloto_residencia
        ),

        aluno AS (
            SELECT a.alu_id, a.cpf,
            COALESCE(
                `rj-crm-registry.udf.VALIDATE_AND_FORMAT_PHONE`(a.Contato_Responsavel),
                `rj-crm-registry.udf.VALIDATE_AND_FORMAT_PHONE`(a.celular_1),
                `rj-crm-registry.udf.VALIDATE_AND_FORMAT_PHONE`(a.celular_2)
            ) AS telefone_sme,
            c.telefone as telefone_escola
            FROM `rj-sme.gestao_escolar.vw_bi_aluno` a
            left join `rj-sme.gestao_escolar.turma` t on a.tur_id = t.id_turma
            left join `rj-sme.educacao_basica.escola` e on t.id_escola = e.id_escola
            left join
                `rj-sme-dev.Inscricoes_site_26_creche.celulares_escolas` c
                on cast(e.id_designacao as string)
                = lpad(cast(c.designacao as string), 7, '0')
            WHERE a.ano = 2026 and t.ano = 2026
                AND a.situacao = 'Ativo'
        ),

        pic_aluno AS (
            SELECT
                educacao.matricula.id_aluno AS alu_id,
                pic.status AS pic_status,
                pic.grupo AS pic_grupo,
                pic.cohort AS pic_cohort
            FROM `rj-pic-dev.projeto_pequenos_cariocas.participantes`
            WHERE educacao.matricula.id_aluno IS NOT NULL
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY educacao.matricula.id_aluno
                ORDER BY IF(pic.status = 'ativo', 0, 1), pic.cohort DESC
            ) = 1
        ),

        pic_cpf AS (
            SELECT
                cpf,
                pic.status AS pic_status,
                pic.grupo AS pic_grupo,
                pic.cohort AS pic_cohort
            FROM `rj-pic-dev.projeto_pequenos_cariocas.participantes`
            WHERE cpf IS NOT NULL
                AND TRIM(cpf) != ''
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY cpf
                ORDER BY IF(pic.status = 'ativo', 0, 1), pic.cohort DESC
            ) = 1
        ),

        contatos_qualificados AS (
            SELECT
                NULLIF(TRIM(CAST(p.cpf AS STRING)), '') AS cpf_crianca,
                LPAD(CAST(r.cpf AS STRING), 11, '0') AS cpf_responsavel_familiar,
                COALESCE(
                    NULLIF(TRIM(r.nome_social), ''),
                    NULLIF(TRIM(r.nome), '')
                ) AS nome_responsavel_familiar,
                `rj-crm-registry`.udf.VALIDATE_AND_FORMAT_PHONE(
                    CONCAT(
                        IFNULL(r.telefone.principal.ddi, '55'),
                        IFNULL(r.telefone.principal.ddd, '21'),
                        r.telefone.principal.valor
                    )
                ) AS telefone_responsavel_familiar,
                r.telefone.principal.estrategia_envio AS estrategia_envio
            FROM `rj-crm-registry.projeto_pequenos_cariocas.participantes` AS p
            INNER JOIN `rj-crm-registry.rmi_dados_mestres.pessoa_fisica` AS r
                ON LPAD(
                    CAST(p.assistencia_social.cadunico.responsavel_familiar.cpf AS STRING),
                    11,
                    '0'
                ) = LPAD(CAST(r.cpf AS STRING), 11, '0')
            WHERE r.telefone.principal.tipo = 'CELULAR'
                AND r.telefone.principal.estrategia_envio IN ('ENVIAR', 'TESTAR', 'EVITAR')
                and r.obito.indicador is false
        ),

        contato_por_crianca AS (
            SELECT
                cpf_crianca,
                nome_responsavel_familiar,
                telefone_responsavel_familiar,
                cpf_responsavel_familiar
            FROM contatos_qualificados
            WHERE cpf_crianca IS NOT NULL
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY cpf_crianca
                ORDER BY
                    telefone_responsavel_familiar IS NULL,
                    CASE estrategia_envio
                        WHEN 'ENVIAR' THEN 1
                        WHEN 'TESTAR' THEN 2
                        WHEN 'EVITAR' THEN 3
                    END,
                    nome_responsavel_familiar IS NULL,
                    cpf_responsavel_familiar,
                    telefone_responsavel_familiar,
                    nome_responsavel_familiar
            ) = 1
        )

    SELECT
        b.data_referencia,
        CURRENT_TIMESTAMP() AS data_processamento,
        b.ano_letivo,
        b.id_escola,
        b.esc_codigo,
        b.tipo_unidade,
        b.esc_nome,
        b.bairro_escola,
        b.ra_escola,
        b.bairro_residencia,
        b.ra_residencia,
        b.grupamento,
        b.turma,
        b.alu_id,
        b.matricula,
        `rj-crm-registry.udf.FORMAT_NAME`(b.nome, true) as nome_aluno,
        `rj-crm-registry.udf.FORMAT_NAME`(b.nome, true) as nome_aluno_2,
        NULLIF(TRIM(a.cpf), '0') AS cpf_aluno,
        COALESCE(REGEXP_CONTAINS(TRIM(a.cpf), r'^\d{11}$'), FALSE) AS cpf_aluno_valido,
        ct.cpf_responsavel_familiar as cpf,
        COALESCE(REGEXP_CONTAINS(TRIM(ct.cpf_responsavel_familiar), r'^\d{11}$'), FALSE) AS cpf_responsavel_familiar_valido,
        `rj-crm-registry.udf.FORMAT_NAME`(ct.nome_responsavel_familiar, true) as nome_responsavel,
        coalesce(a.telefone_sme, ct.telefone_responsavel_familiar) as telefone,
        b.aulas_acum,
        b.faltas_acum,
        b.freq_acum_pct,
        b.dias_30,
        b.faltas_30,
        b.freq_30_letivos_pct,
        b.baixa_freq_acum,
        b.baixa_freq_30,
        b.baixa_freq_ambos,
        b.em_ra_piloto_escola,
        b.em_ra_piloto_residencia,
        (pa.alu_id IS NOT NULL OR pc.cpf IS NOT NULL) AS is_participante_pic,
        COALESCE(pa.pic_status, pc.pic_status) AS pic_status,
        COALESCE(pa.pic_grupo, pc.pic_grupo) AS pic_grupo,
        COALESCE(pa.pic_cohort, pc.pic_cohort) AS pic_cohort
    FROM base AS b
    INNER JOIN aluno AS a
        ON a.alu_id = b.alu_id
    INNER JOIN contato_por_crianca AS ct
        ON ct.cpf_crianca = NULLIF(TRIM(a.cpf), '0')
    LEFT JOIN pic_aluno AS pa
        ON pa.alu_id = b.alu_id
    LEFT JOIN pic_cpf AS pc
        ON pc.cpf = NULLIF(TRIM(a.cpf), '0')
    WHERE telefone_escola IS NOT NULL 
        and b.nome is not null
        and b.matricula is not null
        and ct.cpf_responsavel_familiar is not null
        and ct.cpf_responsavel_familiar != '0'
        and ct.cpf_responsavel_familiar != ''
        ;

    ASSERT (
        SELECT
            COUNT(*) = COUNT(DISTINCT alu_id)
            AND COUNTIF(ra_residencia IS NULL) = 0
        FROM elegiveis
    ) AS 'A lista deve ter uma linha por aluno, com ID e território preenchidos.';

    CREATE TEMP TABLE sorteio AS
    WITH metades AS (
        SELECT
            e.*,
            NTILE(2) OVER (
                PARTITION BY ra_residencia, is_participante_pic
                ORDER BY
                    FARM_FINGERPRINT(CONCAT(semente, '|aluno|', CAST(alu_id AS STRING))),
                    alu_id
            ) AS metade,
            1 + ABS(MOD(FARM_FINGERPRINT(CONCAT(
                semente,
                '|estrato|',
                ra_residencia,
                '|',
                CAST(is_participante_pic AS STRING)
            )), 2)) AS metade_tratamento
        FROM elegiveis AS e
    )
    SELECT
        * EXCEPT (metade, metade_tratamento),
        IF(metade = metade_tratamento, 'tratamento', 'controle') AS grupo_sorteio
    FROM metades;

    -- Conferência por território e participação no PIC.
    SELECT
        ra_residencia,
        is_participante_pic,
        COUNTIF(grupo_sorteio = 'tratamento') AS tratamento,
        COUNTIF(grupo_sorteio = 'controle') AS controle,
        COUNT(*) AS total
    FROM sorteio
    GROUP BY ra_residencia, is_participante_pic
    ORDER BY ra_residencia, is_participante_pic;

    -- Lista completa com matrícula, responsável, telefone e grupo.
    CREATE OR REPLACE TABLE `rj-crm-registry.ab_test.sme_pic_infrequencia_escolar` AS
    SELECT *
    FROM sorteio
    ORDER BY ra_residencia, is_participante_pic, grupo_sorteio, alu_id;


select
cpf, telefone, nome_responsavel, nome_aluno, nome_aluno_2, telefone_escola, matricula
from `rj-crm-registry.ab_test.sme_pic_infrequencia_escolar`
where grupo_sorteio='tratamento'