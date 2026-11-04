SELECT id_sessao, resumo, secretaria_principal, macrotema_principal, conteudo_relevante, causa_nome
FROM `{{ table }}`
WHERE conteudo_relevante = true
