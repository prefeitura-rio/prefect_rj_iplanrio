-- Residentes nos bairros selecionados, com os filtros da seção RMI
-- sem óbito, sem menor, sem NÃO ENVIAR

SELECT
    pf.cpf AS cpf,
    COALESCE(pf.nome_social, pf.nome) AS nome_sobrenome,
    pf.endereco.principal.bairro AS bairro,
    `rj-crm-registry.udf.VALIDATE_AND_FORMAT_CELLPHONE`(
        CONCAT(
            IFNULL(pf.telefone.principal.ddi, "55"),
            IFNULL(pf.telefone.principal.ddd, "21"),
            pf.telefone.principal.valor
        )
    ) AS telefone,

    CASE pf.endereco.principal.bairro
        WHEN 'São Cristóvão' THEN 'Quinta da Boa Vista / São Cristóvão'
        WHEN 'Humaitá' THEN 'Humaitá'
        WHEN 'Maracanã' THEN 'Maracanã'
        WHEN 'Tijuca' THEN 'Maracanã'
        WHEN 'Madureira' THEN 'Madureira'
        WHEN 'Oswaldo Cruz' THEN 'Madureira'
        WHEN 'Barra da Tijuca' THEN 'Praia da Reserva'
        WHEN 'Recreio dos Bandeirantes' THEN 'Praia da Reserva'
        WHEN 'Campo Grande' THEN 'Campo Grande'
    END AS Local,

    CASE pf.endereco.principal.bairro
        WHEN 'São Cristóvão' THEN 'frequenta a Quinta da Boa Vista'
        WHEN 'Humaitá' THEN 'estaciona no Humaitá'
        WHEN 'Maracanã' THEN 'estaciona no entorno do Maracanã'
        WHEN 'Tijuca' THEN 'estaciona no entorno do Maracanã'
        WHEN 'Madureira' THEN 'estaciona em Madureira'
        WHEN 'Oswaldo Cruz' THEN 'estaciona em Madureira'
        WHEN 'Barra da Tijuca' THEN 'frequenta a Praia da Reserva'
        WHEN 'Recreio dos Bandeirantes' THEN 'frequenta a Praia da Reserva'
        WHEN 'Campo Grande' THEN 'circula por Campo Grande'
    END AS relacao_local,

    CASE pf.endereco.principal.bairro
        WHEN 'São Cristóvão' THEN 113
        WHEN 'Humaitá' THEN 331
        WHEN 'Maracanã' THEN 240
        WHEN 'Tijuca' THEN 240
        WHEN 'Madureira' THEN 245
        WHEN 'Oswaldo Cruz' THEN 245
        WHEN 'Barra da Tijuca' THEN 1800
        WHEN 'Recreio dos Bandeirantes' THEN 1800
        WHEN 'Campo Grande' THEN 250
    END AS numero_vagas,

    CASE pf.endereco.principal.bairro
        WHEN 'São Cristóvão' THEN 'nas ruas Dom Meinrado e General Herculano Gomes e na Avenida do Exército'
        WHEN 'Humaitá' THEN 'nas ruas Humaitá, Marques, Conde de Irajá, Martins Ferreira e Capistrano de Abreu'
        WHEN 'Maracanã' THEN 'nas ruas General Canabarro, Ibituruna, Morais e Silva, Pedro Guedes e Visconde de Cairu'
        WHEN 'Tijuca' THEN 'nas ruas General Canabarro, Ibituruna, Morais e Silva, Pedro Guedes e Visconde de Cairu'
        WHEN 'Madureira' THEN 'nas ruas Antônio de Abreu, Clara Nunes, Maria de Freitas, Dagmar da Fonseca, Américo Brasiliense, Manuel Marques, Alcina, São Geraldo e Dona Clara'
        WHEN 'Oswaldo Cruz' THEN 'nas ruas Antônio de Abreu, Clara Nunes, Maria de Freitas, Dagmar da Fonseca, Américo Brasiliense, Manuel Marques, Alcina, São Geraldo e Dona Clara'
        WHEN 'Barra da Tijuca' THEN 'ao longo da Avenida Lúcio Costa, no trecho da Praia da Reserva'
        WHEN 'Recreio dos Bandeirantes' THEN 'ao longo da Avenida Lúcio Costa, no trecho da Praia da Reserva'
        WHEN 'Campo Grande' THEN 'nas ruas Augusto Vasconcelos, Amaral Costa, Agostinho Coelho, Engenheiro Trindade, José Ferreira, Guaraí, Uruna, Tatuoca, Viúva Dantas, Doutor Caetano de Faria Castro, Mário Barbosa, Camanducaia e Sacramento Blake'
    END AS logradouros

FROM `rj-crm-registry.rmi_dados_mestres.pessoa_fisica` pf
-- FROM `rj-rmi.rmi_dados_mestres.pessoa_fisica` pf
WHERE pf.cpf IS NOT NULL
    AND pf.telefone.principal.estrategia_envio != 'NÃO ENVIAR'
    AND pf.obito.indicador IS FALSE
    AND pf.menor_idade IS FALSE
    AND pf.endereco.principal.bairro IN (
        'São Cristóvão',
        'Humaitá',
        'Maracanã',
        'Tijuca',
        'Madureira',
        'Oswaldo Cruz',
        'Barra da Tijuca',
        'Recreio dos Bandeirantes',
        'Campo Grande'
    )
;