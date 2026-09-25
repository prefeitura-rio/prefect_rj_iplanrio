-- 1 linha por TELEFONE, com a lista dos CPFs do universo `alvo` (ver amostra_agora.sql)
-- ligados a ele. A probabilidade só depende das features do telefone, então o Python
-- pontua cada telefone uma vez e repete o resultado para cada CPF da lista.
SELECT c.cpfs, f.*
FROM features f
JOIN (SELECT telefone, ARRAY_AGG(cpf) AS cpfs FROM alvo GROUP BY telefone) c
  ON c.telefone = f.telefone
