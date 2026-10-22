# Atividade Econômica - SMFP

Copia as tabelas do banco `DW_BI_ALVARAS` (Alvarás, SQL Server) da SMFP para o BigQuery, no dataset `atividade_economica`.

O `flow.py` é o `dump_database` de `utils.flows`. Os parâmetros ficam no `prefect.yaml`: o banco e o `cloud` em cada `deployment`, e uma tabela em cada `schedule`. Veja o [README da raiz](../../README.md#flows-prontos).

## Tabelas

- Fatos: `fact_fatoalvaras` e `fact_fatocp`.
- Dimensões: `tab_alvara`, `tab_atvprocesso`, `tab_cae`, `tab_cnae`, `tab_consulta`, `tab_direcionamento`, `tab_tipocontribuinte_tipocontribuint` e `tab_tiposolicitacao`.

Cada tabela usa `dump_mode: overwrite` e a consulta do próprio `schedule`. Os `schedules` estão com `active: false`: ligue um para rodar a tabela todo dia.

## Credenciais

O usuário e a senha vêm de `DB_USERNAME` e `DB_PASSWORD`, no `Secret` do `deployment` (`secretName`). Eles são os do Infisical `/db-alvaras`.

## Adicionar uma tabela

Copie um `schedule` do `prefect.yaml` e troque `slug`, `table.table_id` e `read.query`.
