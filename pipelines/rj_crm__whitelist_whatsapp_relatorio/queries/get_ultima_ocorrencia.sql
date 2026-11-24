-- Instante da ocorrência mais recente com URL identificada, em toda a tabela.
-- Fonte em `$project.$dataset_id.$table_id`: mesmos replacements da query da janela (§7.5),
-- para que as duas não possam apontar para tabelas diferentes por descuido.
-- Deliberadamente sem recorte de janela: o valor serve para desambiguar o período vazio.
-- Sem ele, "não houve ocorrência" e "a fonte parou de gerar ocorrências" são
-- indistinguíveis, e o relatório pode ficar verde e vazio indefinidamente.
-- Mesma definição de ocorrência de get_redacted_urls.sql: redacted_urls é um array JSON com
-- pelo menos um item. Cobre os modos de quebra: a chave sumir do JSON, mudar de formato ou
-- continuar presente mas vazia (null, [] ou "") porque a redação foi desligada. Em todos, a
-- data congela e a distância até hoje cresce sozinha, à vista de quem lê o canal.
-- O marcador URL_Redacted em saida_valor_texto não entra: desde 2026-09-08 essa coluna chega
-- NULL nos passos com URL redigida.
-- Só é executada em dia sem ocorrências, então a varredura extra não incide nos dias em
-- que o relatório tem conteúdo.
SELECT MAX(inicio_datahora) AS ultima_ocorrencia
FROM `$project.$dataset_id.$table_id`
WHERE JSON_TYPE(passo_atributos_json.redacted_urls) = 'array'
  AND ARRAY_LENGTH(JSON_QUERY_ARRAY(passo_atributos_json.redacted_urls)) > 0
