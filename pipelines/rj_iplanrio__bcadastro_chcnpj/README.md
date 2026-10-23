# rj_iplanrio__bcadastro_chcnpj

Copia a base `chcnpj_bcadastros` do CouchDB do bcadastro (empresas (CNPJ)) para a tabela `brutos_bcadastro.chcnpj` do BigQuery, em uma tabela Delta no bucket.

- O flow é o `dump_database` de `utils.flows`. Todo o desenho está no `prefect.yaml`.
- As colunas são as da forma `utils.couchdb:BCADASTRO`: `id`, `doc` (JSON), `key`, `seq`, `value` e `last_seq`, como a tabela `chcnpj_bcadastros` do Airbyte, sem as colunas `_airbyte_`.
- A leitura é o `_all_docs` da base, cortado em faixas de ids. O CouchDB fica na rede interna (`10.2.231.241:6984`), então só o worker `onprem-pool` o alcança.
- O usuário e a senha vêm de `DB_USERNAME` e `DB_PASSWORD`, na pasta `/rj_iplanrio__bcadastro_chcnpj` do projeto `pipelines` do Infisical.
- Toda execução lê a base inteira. Um chunk que não mudou não é escrito de novo. Para uma carga incremental, use `read.since` com o `last_seq` da carga anterior.
- A tabela `brutos_bcadastro_staging.chcnpj` é nova e não substitui a `chcnpj_bcadastros` do Airbyte.
