# Infraestrutura SISCOR Obras - SECONSERVA

Copia os dados do `SISCOR` (Sistema de Conservação e Obras, SQL Server, base `siscor_seconserva`) para o BigQuery, no dataset `infraestrutura_siscor_obras`. O `SISCOR` guarda os processos de autorização de obras públicas e privadas: requerentes, executores, pareceres, localização e licenças.

O `flow.py` é o `dump_database` de `utils.flows`. Os parâmetros ficam no `prefect.yaml`: o banco e o `cloud` em cada `deployment`, e uma tabela em cada `schedule`. Veja o [README da raiz](../../README.md#flows-prontos).

## Tabelas

- `processo_autorizacao_obra`: roda todo dia, em `overwrite`, com a consulta do `schedule`.

## Credenciais

O usuário e a senha vêm de `DB_USERNAME` e `DB_PASSWORD`, no `Secret` do `deployment` (`secretName`). Eles são os do Infisical `/db-siscor`.

## Adicionar uma tabela

Copie o `schedule` do `prefect.yaml` e troque `slug`, `table.table_id` e `read.query`.
