-- Remove linhas de uma execução anterior da MESMA versão, pra reprocessar sem duplicar —
-- ver tasks/retreino/publicar.py::publica_avaliacao_bq (delete + append, não upsert: uma
-- versão nunca é reprocessada parcialmente, sempre a tabela toda daquela execução).
-- @versao é um query PARAMETER (um valor, não um identificador de tabela — por isso não
-- usa o mesmo placeholder de template que project/dataset_id/table_id, ver seção 7.2 do
-- STYLEGUIDE sobre variável de template vs. parâmetro de query). CUIDADO: não escrever o
-- cifrão desse outro placeholder aqui em comentário — string.Template varre o arquivo
-- INTEIRO atrás de cifrão+identificador, inclusive dentro de comentário, e quebra com
-- KeyError se o nome não for passado a load_query (foi exatamente esse bug, corrigido
-- 2026-09-24).
DELETE FROM `$project.$dataset_id.$table_id`
WHERE versao = @versao
