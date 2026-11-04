MERGE `{{ destination }}` AS t
USING `{{ tmp }}` AS s
ON t.id_sessao = s.id_sessao
WHEN MATCHED THEN
    UPDATE SET t.tema_nome = s.tema_nome, t.causa_nome = s.causa_nome
