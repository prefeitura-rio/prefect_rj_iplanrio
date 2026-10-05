-- Instante da ocorrência mais recente com URL identificada, em toda a tabela.
-- Fonte em `$project.$dataset_id.$table_id`: mesmos replacements da query da janela (§7.5),
-- para que as duas não possam apontar para tabelas diferentes por descuido.
-- Deliberadamente sem recorte de janela: o valor serve para desambiguar o período vazio.
-- Sem ele, "não houve ocorrência" e "a fonte parou de gerar ocorrências" são
-- indistinguíveis, e o relatório pode ficar verde e vazio indefinidamente.
-- Cobre os dois modos de quebra: se o marcador URL_Redacted mudar, o LIKE deixa de casar;
-- se a chave redacted_urls sumir do JSON, o IS NOT NULL deixa de casar. Nos dois casos a
-- data congela e a distância até hoje cresce sozinha, à vista de quem lê o canal.
-- Só é executada em dia sem ocorrências, então a varredura extra não incide nos dias em
-- que o relatório tem conteúdo.
SELECT MAX(inicio_datahora) AS ultima_ocorrencia
FROM `$project.$dataset_id.$table_id`
WHERE saida_valor_texto LIKE '%URL_Redacted%'
  AND passo_atributos_json.redacted_urls IS NOT NULL
