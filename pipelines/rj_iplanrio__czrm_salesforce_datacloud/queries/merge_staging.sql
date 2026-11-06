-- MERGE da staging para a tabela final, por chave primária.
--
-- Diferente da pipeline do CRM (que casa chave + data_particao porque são
-- tabelas de evento), aqui a chave é só o id: Account e WorkOrder são
-- cadastro — uma linha por registro, atualizada no lugar. A partição
-- (data_criacao) é atributo do registro e nunca muda, então não entra no ON.
--
-- A staging é deduplicada por chave antes do MERGE (fica a linha de maior
-- _loaded_at): a fonte não deve repetir id, mas se repetir o MERGE falha com
-- "must match at most one source row" e, como a staging só é limpa após um
-- MERGE bem-sucedido, a duplicata travaria todos os ticks seguintes.
--
-- _first_loaded_at não existe na staging (só na final): entra no INSERT com o
-- _loaded_at da carga e nunca é tocado no UPDATE.
MERGE `$target` AS t
USING (
    SELECT *
    FROM `$staging`
    WHERE TRUE
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY $primary_key
        ORDER BY _loaded_at DESC
    ) = 1
) AS s
ON t.$primary_key = s.$primary_key
WHEN MATCHED THEN
    UPDATE SET $set_clause
WHEN NOT MATCHED THEN
    INSERT ($insert_cols, _first_loaded_at)
    VALUES ($insert_vals, s._loaded_at)
