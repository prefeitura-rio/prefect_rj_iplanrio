SELECT nsu, xml_sha256, origem
FROM `${projeto}.${dataset}.documentos`
WHERE ambiente = @ambiente
  AND nsu BETWEEN @nsu_ini AND @nsu_fim
  AND data_hora_geracao >= TIMESTAMP '1970-01-01'
ORDER BY nsu
