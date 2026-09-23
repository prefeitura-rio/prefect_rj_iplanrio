-- Remove linhas de uma execução anterior da MESMA versão, pra reprocessar sem duplicar —
-- ver tasks/retreino/publicar.py::publica_avaliacao_bq (delete + append, não upsert: uma
-- versão nunca é reprocessada parcialmente, sempre a tabela toda daquela execução).
-- @versao é um query PARAMETER (um valor, não um identificador de tabela — por isso não
-- é $versao, ver seção 7.2 do STYLEGUIDE sobre $variable vs. parâmetro de query).
DELETE FROM `$project.$dataset_id.$table_id`
WHERE versao = @versao
