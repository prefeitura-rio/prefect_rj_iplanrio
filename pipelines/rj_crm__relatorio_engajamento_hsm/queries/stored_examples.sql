-- Exemplos revisados de todas as campanhas, com os embeddings.
-- A tabela é Delta, publicada no BigQuery por uma conexão BigLake. O flow lê tudo porque
-- grava a tabela inteira de volta (mode overwrite) sob o limite de concorrência 1.
select
    campaign,
    session_id,
    conversation,
    categories,
    review,
    review_note,
    file_id,
    file_name,
    modified_at,
    revision_date,
    embedding,
    model,
    ingested_at
from `{{ examples_table }}`
