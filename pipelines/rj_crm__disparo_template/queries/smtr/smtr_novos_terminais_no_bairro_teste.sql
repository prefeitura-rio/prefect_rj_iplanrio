-- TESTE - SMTR - Novo terminal no bairro (Template 4)
-- Mesmos textos de config_campanhas da query de produção
-- (smtr_novos_terminais_no_bairro.sql), enviados só para os contatos de teste.
-- Cada contato recebe uma frente (prioridade): troque o número em contatos_teste para
-- ver as outras versões (1 Campo Grande, 2 Santa Cruz, 3 Colégio, 4 Coelho Neto).
WITH
  config_campanhas AS (
    SELECT * FROM UNNEST([
      STRUCT(
        1 AS prioridade,
        'Campo Grande' AS bairro,
        'um novo Terminal Intermodal, entre a Estrada da Caroba e a Rua Campo Grande' AS terminal,
        'Serão 33 linhas municipais e intermunicipais, com conexão ao BRT, plataformas cobertas, acessibilidade, banheiros e bicicletário.' AS detalhes
      ),
      STRUCT(
        2 AS prioridade,
        'Santa Cruz' AS bairro,
        'o Terminal Bairro Imperial, reunindo ônibus, BRT e vans no centro do bairro, perto das ruas Álvaro Alberto, Felipe Cardoso e Dom Pedro I' AS terminal,
        'Vai ter plataformas organizadas, acessibilidade, banheiros, lojas e bicicletário para até 300 vagas. 📅 A previsão é começar a funcionar no segundo semestre de 2027. Mais facilidade pra fazer sua conexão e seguir viagem! 💙' AS detalhes
      ),
      STRUCT(
        3 AS prioridade,
        'Colégio' AS bairro,
        'um terminal junto à estação de metrô, com seis pontos de ônibus cobertos e espaço organizado pra embarcar e desembarcar' AS terminal,
        'Vai ter cobertura e melhorias no entorno, com duas quadras e área de lazer pra criançada.' AS detalhes
      ),
      STRUCT(
        4 AS prioridade,
        'Coelho Neto' AS bairro,
        'um terminal junto à estação de metrô, reunindo ônibus municipais e intermunicipais em um espaço organizado pra embarcar e desembarcar' AS terminal,
        'A novidade vai facilitar a viagem de passageiros da zona norte, zona oeste, Itaguaí, Seropédica e Cabuçu.' AS detalhes
      )
    ])
  ),

  contatos_teste AS (
    SELECT * FROM UNNEST([
      STRUCT('5511984677798' AS telefone, '00000000350' AS cpf, 1 AS prioridade),
      STRUCT('5521980375732' AS telefone, '00000000349' AS cpf, 2 AS prioridade),
      STRUCT('5521989190512' AS telefone, '00000000032' AS cpf, 3 AS prioridade)
    ])
  )

SELECT
  t.telefone,
  t.cpf,
  cfg.bairro,
  cfg.terminal,
  cfg.detalhes
FROM contatos_teste AS t
INNER JOIN config_campanhas AS cfg
  ON cfg.prioridade = t.prioridade
