-- O cursor só avança: um run atrasado nunca o faz recuar.
MERGE `${projeto}.${dataset}.adn_cursor` AS alvo
USING (SELECT @ambiente AS ambiente, @ultimo_nsu AS ultimo_nsu, @atualizado_em AS atualizado_em) AS origem
ON alvo.ambiente = origem.ambiente
WHEN MATCHED AND alvo.ultimo_nsu < origem.ultimo_nsu THEN
  UPDATE SET ultimo_nsu = origem.ultimo_nsu, atualizado_em = origem.atualizado_em
WHEN NOT MATCHED THEN
  INSERT (ambiente, ultimo_nsu, atualizado_em) VALUES (origem.ambiente, origem.ultimo_nsu, origem.atualizado_em)
