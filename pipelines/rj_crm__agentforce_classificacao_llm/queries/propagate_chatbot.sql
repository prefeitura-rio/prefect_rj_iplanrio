MERGE `{{ chatbot_table }}` AS t
USING `{{ source }}` AS s
ON t.id_interacao = s.id_sessao
WHEN MATCHED THEN
    UPDATE SET t.llm.tema_nome = s.tema_nome, t.llm.causa_nome = s.causa_nome
