with
    rmi as (
        select
            pf.cpf,
            `rj-crm-registry.udf.VALIDATE_AND_FORMAT_PHONE`(
                concat(
                    ifnull(pf.telefone.principal.ddi, '55'),
                    ifnull(pf.telefone.principal.ddd, '21'),
                    pf.telefone.principal.valor
                )
            ) as telefone_rmi
        from `rj-crm-registry.rmi_dados_mestres.pessoa_fisica` pf
        where
            1 = 1
            -- filtros obrigatórios de disparo
            and pf.menor_idade is false
            and pf.obito.indicador is false
            and pf.telefone.principal.qualidade = 'VALIDO'
            and pf.telefone.principal.estrategia_envio in ('ENVIAR', 'TESTAR')
            and coalesce(pf.telefone.principal.indicador_optout, false) = false
            and coalesce(pf.telefone.principal.indicador_quarentena, false) = false
    ),

    dados as (
        select
            b.matricula as matricula,
            `rj-crm-registry.udf.FORMAT_NAME`(b.nome, true) as nome_aluno,
            `rj-crm-registry.udf.FORMAT_NAME`(b.nome, true) as nome_aluno_2,
            coalesce(
                `rj-crm-registry.udf.VALIDATE_AND_FORMAT_PHONE`(a.contato_responsavel),
                `rj-crm-registry.udf.VALIDATE_AND_FORMAT_PHONE`(a.celular_1),
                `rj-crm-registry.udf.VALIDATE_AND_FORMAT_PHONE`(a.celular_2),
                `rj-crm-registry.udf.VALIDATE_AND_FORMAT_PHONE`(telefone_rmi)
            ) as telefone,
            a.cpf_responsavel as subscriberkey,
            `rj-crm-registry.udf.FORMAT_NAME`(a.nome_responsavel, true) as nome_responsavel,
            -- Celular da escola
            `rj-crm-registry.udf.FORMAT_PHONE_DISPLAY`(
                cast(c.telefone as string)
            ) as telefone_escola
        from rj-sme.gestao_escolar.vw_bi_aluno a
        left join rmi on rmi.cpf = a.cpf_responsavel
        left join rj-sme-dev.Inscricoes_site_26_creche.basecoc2infreq b
            on trim(cast(b.matricula as string)) = trim(a.matricula)
        left join rj-sme.gestao_escolar.turma t on a.tur_id = t.id_turma
        left join rj-sme.educacao_basica.escola e on t.id_escola = e.id_escola
        left join
            `rj-sme-dev.Inscricoes_site_26_creche.celulares_escolas` c
            on cast(e.id_designacao as string)
            = lpad(cast(c.designacao as string), 7, '0')
        where
            1 = 1
            and upper(trim(b.grupamento)) in (
                '6º ANO', '7º ANO', '8º ANO', '9º ANO', 'CARIOCA I', 'CARIOCA II'
            )
            and a.situacao = 'Ativo'
            and a.cpf_responsavel is not null
            and a.cpf_responsavel != '0'
            and a.cpf_responsavel != ''
            and length(cast(a.cpf as string)) = 11
            and c.telefone is not null
            and b.matricula is not null
            and b.nome is not null
        group by 1, 2, 3, 4, 5, 6, 7
        order by a.cpf_responsavel
    ),

    -- Alunos já notificados: disparo confirmado com CPF + telefone + nome_aluno
    -- correspondentes a uma linha desta campanha
    ja_notificados as (
        select
            conv.contato.cpf as subscriberkey,
            json_value(conv.hsm.dados_disparo, '$.nome_aluno') as nome_aluno
        from `rj-crm-registry.rmi_conversas.v2_chatbot_conversas` conv
        where
            conv.hsm.indicador is true
            and conv.contato.cpf is not null
            and json_value(conv.hsm.dados_disparo, '$.nome_aluno') is not null
            and data_particao >= "2026-09-10"
    )

select d.*
from dados d
where
    d.telefone is not null
    -- exclui alunos cujo responsável (CPF) já recebeu disparo para este aluno específico (nome_aluno)
    and not exists (
        select 1
        from ja_notificados jn
        where
            jn.subscriberkey = d.subscriberkey
            and jn.nome_aluno = d.nome_aluno
    )