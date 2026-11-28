-- TESTE - SMTR - Rio Rotativo Digital - Novas vagas
-- Mesmos textos de config_campanhas da query de produção
-- (smtr_rio_rotativo_novas_vagas.sql), enviados só para os contatos de teste.
-- Cada contato recebe uma frente (prioridade 1 a 9): troque o número em contatos_teste para
-- ver outra versão. O flow filtra CPF duplicado, então um contato recebe uma frente por vez.
WITH
  config_campanhas AS (
    SELECT * FROM UNNEST([
      STRUCT(
        1 AS prioridade,
        ['IMPERIAL DE SAO CRISTOVAO', 'SAO CRISTOVAO'] AS bairros,
        'frequenta a Quinta da Boa Vista' AS relacao_local,
        '113' AS numero_vagas,
        'nas ruas Dom Meinrado e General Herculano Gomes e na Avenida do Exército' AS logradouros
      ),
      STRUCT(
        2 AS prioridade,
        ['HUMAITA', 'BOTAFOGO'] AS bairros,
        'estaciona no Humaitá' AS relacao_local,
        '331' AS numero_vagas,
        'nas ruas Humaitá, Marques, Conde de Irajá, Martins Ferreira e Capistrano de Abreu' AS logradouros
      ),
      STRUCT(
        3 AS prioridade,
        ['MARACANA'] AS bairros,
        'estaciona no entorno do Maracanã' AS relacao_local,
        '240' AS numero_vagas,
        'nas ruas General Canabarro, Ibituruna, Morais e Silva, Pedro Guedes e Visconde de Cairu' AS logradouros
      ),
      STRUCT(
        4 AS prioridade,
        ['MADUREIRA', 'OSVALDO CRUZ', 'OSWALDO CRUZ'] AS bairros,
        'estaciona em Madureira' AS relacao_local,
        '245' AS numero_vagas,
        'nas ruas Antônio de Abreu, Clara Nunes, Maria de Freitas, Dagmar da Fonseca, Américo Brasiliense, Manuel Marques, Alcina, São Geraldo e Dona Clara' AS logradouros
      ),
      STRUCT(
        5 AS prioridade,
        ['RECREIO DOS BANDEIRANTES'] AS bairros,
        'frequenta a Praia da Reserva' AS relacao_local,
        '1.800' AS numero_vagas,
        'ao longo da Avenida Lúcio Costa, no trecho da Praia da Reserva' AS logradouros
      ),
      STRUCT(
        6 AS prioridade,
        ['CAMPO GRANDE'] AS bairros,
        'circula por Campo Grande' AS relacao_local,
        '250' AS numero_vagas,
        'nas ruas Augusto Vasconcelos, Amaral Costa, Agostinho Coelho, Engenheiro Trindade, José Ferreira, Guaraí, Uruna, Tatuoca, Viúva Dantas, Doutor Caetano de Faria Castro, Mário Barbosa, Camanducaia e Sacramento Blake' AS logradouros
      ),
      STRUCT(
        7 AS prioridade,
        ['LEME', 'COPACABANA', 'IPANEMA', 'LEBLON'] AS bairros,
        'frequenta as praias do Leme, Copacabana, Ipanema e Leblon' AS relacao_local,
        'mais de 1.700' AS numero_vagas,
        'distribuídas pelos quatro bairros' AS logradouros
      ),
      STRUCT(
        8 AS prioridade,
        ['BARRA DA TIJUCA', 'SAO CONRADO'] AS bairros,
        'frequenta a Barra da Tijuca e São Conrado' AS relacao_local,
        '1.400' AS numero_vagas,
        'na Barra da Tijuca, no trecho entre a Praia dos Amores e o Alvorada, além de vagas em São Conrado' AS logradouros
      ),
      STRUCT(
        9 AS prioridade,
        ['TIJUCA'] AS bairros,
        'circula pela Tijuca' AS relacao_local,
        '187' AS numero_vagas,
        'nas ruas Campos Sales, Afonso Pena, Martins Pena e Marechal Marques Porto' AS logradouros
      )
    ])
  ),

  contatos_teste AS (
    SELECT * FROM UNNEST([
      STRUCT('5511984677798' AS telefone, '00000000350' AS cpf, 'patricia' AS nome, 1 AS prioridade),
      STRUCT('5511984677798' AS telefone, '00000000351' AS cpf, 'patricia' AS nome, 2 AS prioridade),
      STRUCT('5511984677798' AS telefone, '00000000352' AS cpf, 'patricia' AS nome, 3 AS prioridade),
      STRUCT('5511984677798' AS telefone, '00000000353' AS cpf, 'patricia' AS nome, 4 AS prioridade),
      STRUCT('5511984677798' AS telefone, '00000000354' AS cpf, 'patricia' AS nome, 5 AS prioridade),
      STRUCT('5511984677798' AS telefone, '00000000355' AS cpf, 'patricia' AS nome, 6 AS prioridade),
      STRUCT('5511984677798' AS telefone, '00000000356' AS cpf, 'patricia' AS nome, 7 AS prioridade),
      STRUCT('5511984677798' AS telefone, '00000000357' AS cpf, 'patricia' AS nome, 8 AS prioridade),
      STRUCT('5511984677798' AS telefone, '00000000358' AS cpf, 'patricia' AS nome, 9 AS prioridade)
    ])
  )

SELECT
  t.telefone,
  t.cpf,
  `rj-crm-registry.udf.FORMAT_NAME`(t.nome, TRUE) AS nome_sobrenome,
  cfg.bairros[OFFSET(0)] AS bairro,
  cfg.relacao_local,
  cfg.numero_vagas,
  cfg.logradouros
FROM contatos_teste AS t
INNER JOIN config_campanhas AS cfg
  ON cfg.prioridade = t.prioridade
