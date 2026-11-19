-- O filtro constante em data_hora_geracao satisfaz o require_partition_filter sem excluir nenhuma partição;
-- o corte por nsu aproveita o cluster.
SELECT nsu
FROM `${projeto}.${dataset}.documentos`
WHERE ambiente = @ambiente
  AND nsu BETWEEN @nsu_ini AND @nsu_fim
  AND data_hora_geracao >= TIMESTAMP '1970-01-01'
