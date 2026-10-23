# rj_iplanrio__bcadastro_chcno

Copia a base `chcno_bcadastros` do CouchDB do bcadastro (obras, CNO) para a tabela `brutos_bcadastro.chcno_bcadastros` do BigQuery, em uma tabela Delta no bucket.

- O flow é o `dump_database` de `utils.flows`. Todo o desenho está no `prefect.yaml`.
- As colunas são as da forma `utils.couchdb:BCADASTRO`: `id`, `doc`, `key`, `seq`, `value` e `last_seq`, mais `_prefect_extracted_at` (o horário da carga, no lugar de `_airbyte_extracted_at`) e `batch`. O BigQuery lê `doc` e `value` como texto JSON: `JSON_VALUE(doc, '$.campo')` funciona. `seq` e `last_seq` vêm vazios, porque o `_all_docs` não os tem.
- A leitura é o `_all_docs` da base, cortado em faixas de ids. O `id` do CNO tem 12 dígitos quando é o número da obra. Há também ids de eventos (`CNO-<uuid>`), que vão para o chunk nulo. A faixa de cada chunk tem 10 bilhões de ids. O bloco de ids que começam com `9000` tem 3,1 milhões de documentos e cai em um único chunk.
- O CouchDB fica na rede interna (`10.2.231.241:6984`), então só o worker `onprem-pool` o alcança. O login é o da fonte "CouchDB" do Airbyte.
- O usuário e a senha vêm de `DB_USERNAME` e `DB_PASSWORD`, na pasta `/rj_iplanrio__bcadastro_chcno` do projeto `pipelines` do Infisical.
- Toda execução lê a base inteira e reescreve todos os chunks: `last_seq` é o `update_seq` da base, que muda a cada escrita no CouchDB, e ele entra na impressão digital do chunk, então nenhum chunk é pulado. A base tem 3,68 milhões de documentos. O tempo de uma leitura inteira não foi medido no cluster. Para uma carga incremental, use `read.since` com o `last_seq` da carga anterior.
- A tabela substitui a `chcno_bcadastros` do Airbyte em `brutos_bcadastro_staging`, que é a fonte dos modelos do dbt. O BigQuery não troca uma tabela nativa por uma externa: apague a do Airbyte e desligue a conexão dela antes da primeira execução. Os modelos do dbt leem `_airbyte_extracted_at` : troque por `_prefect_extracted_at`. As outras colunas `_airbyte_*` não existem na tabela nova.
