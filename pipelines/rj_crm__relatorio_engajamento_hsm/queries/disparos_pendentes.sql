-- Disparos com relatório de engajamento pedido pra uma data de referência: têm valor em
-- nome_campanha/relatorio_engajamento_data_disparo E relatorio_engajamento_data_geracao
-- bate com a data de referência (por padrão, hoje — ver flow.py). `distinct` porque
-- disparos_ativos tem linha duplicada por HSM (histórico de ativo/inativo).
--
-- disparos_ativos é uma tabela externa (Google Sheet importada) — a credencial usada
-- pra rodar esta query PRECISA ter escopo Drive, não só BigQuery (ver utils/bigquery.py).
select distinct
    nome_campanha,
    relatorio_engajamento_data_disparo
from `{disparos_table}`
where relatorio_engajamento_data_geracao = @data_referencia
  and relatorio_engajamento_data_disparo is not null
  and nome_campanha is not null
