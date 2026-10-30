# rj_iplanrio__czrm_salesforce_datacloud

Sincroniza objetos do **CZRM (Salesforce)** — via **Data Cloud / Data 360** —
para tabelas brutas no BigQuery, com atualização a cada 15 minutos e upsert
por `MERGE`.

| | |
|---|---|
| **Dataset** | `rj-iplanrio.brutos_czrm_salesforce_staging` (zona de pouso, pré-dbt) |
| **Camada curada** | `rj-iplanrio.brutos_czrm_salesforce` — escrita pelo dbt (`queries-rj-iplanrio`), não por esta pipeline |
| **Tabelas** | `account`, `work_order` (e as próximas que entrarem em `tabelas.yaml`) |
| **Origens** | `qa`, `staging`, `prod` — mudam só a credencial e o nome da DLO |

## Como funciona

1. Descobre as colunas da DLO com `SELECT * LIMIT 0` (o usuário de integração
   não tem permissão de ler metadado de Data Stream, mas a query vazia
   devolve nome **e tipo** de cada coluna).
2. Extrai a janela com `WHERE SystemModstamp__c >= ... AND < ...`, paginando
   por `ORDER BY Id__c LIMIT 1000 OFFSET n`.
3. Transforma: snake_case, datas em UTC, booleanos, `data_criacao` /
   `data_atualizacao` como dia de São Paulo.
4. Carrega na `<tabela>_staging` e faz `MERGE` por `id` na tabela final.
5. Limpa a staging.

A tabela final é **estado atual** (uma linha por registro), particionada por
`data_criacao`, com `_loaded_at` (última carga) e `_first_loaded_at`
(primeira vez que o registro apareceu).

## Schedules (deployment `--prod`)

| Slug | Quando | Janela | Para quê |
|---|---|---|---|
| `czrm-salesforce-15min` | `*/15 * * * *` | última 1h | passe quente |
| `czrm-salesforce-diario` | `15 0 * * *` | dia anterior | rede de segurança do quente |
| `czrm-salesforce-semanal` | `0 3 * * 0` | últimos 7 dias | ingestão atrasada no Data Cloud |

Os três sobem com `active: false` na primeira entrega. `concurrency_limit: 1`
(ENQUEUE) impede duas janelas escrevendo ao mesmo tempo.

O deployment `--staging` **não tem schedule** e roda com `dry_run=True`:
extrai e transforma sem escrever.

## Primeira carga (runbook)

1. Push da branch `staging/...` → deploy automático do `--staging`.
2. Run manual no `--staging` com `origem: qa` (204 contas / 1.720 OS no QA)
   e `dry_run: true` — confere extração e contagens.
3. Merge na `master` → deploy do `--prod` (schedules desligados).
4. Run manual no `--prod`:
   ```json
   { "modo": "carga_inicial", "tabelas": ["work_order"] }
   ```
   Confere a tabela (1 linha hoje em produção).
5. Repete com `"tabelas": ["account"]` (~67 mil linhas).
6. PR seguinte: `active: true` nos três schedules.

`modo=carga_inicial` ignora o filtro de data e lê a tabela inteira. **Não é
schedule** de propósito.

## Credenciais

Infisical, projeto `prefect-jobs`, pasta `czrm-salesforce` — um jogo por org:

```
SF_DC_{QA,STAGING,PROD}_CLIENT_ID
SF_DC_{QA,STAGING,PROD}_CLIENT_SECRET
SF_DC_{QA,STAGING,PROD}_INSTANCE_URL
SF_DC_{QA,STAGING,PROD}_DATASPACE
```

O sync monta `<PASTA_MAIUSCULA>__<CHAVE>` (ex.:
`CZRM_SALESFORCE__SF_DC_PROD_CLIENT_ID`). O código aceita também o nome sem
prefixo. Falta de chave falha com mensagem dizendo **qual origem e qual
chave** procurou.

Autenticação: OAuth2 **client_credentials** (`cdp_api`), sem usuário/senha.

## Notificações

`DISCORD_WEBHOOK_URL_CZRM_SALESFORCE` (opcional) — resumo nos modos frios e
carga inicial, e aviso de falha por tabela. Sem a variável, notificação é
pulada.

## Guard-rails

- **Origem não-prod não grava na zona de pouso de prod.** Se `origem != prod`
  e `dataset_id` = `brutos_czrm_salesforce_staging` sem `dry_run`, o flow
  falha antes de qualquer escrita.
- **Drift de schema:** coluna nova entra sozinha (o schema é redescoberto a
  cada run); coluna que some da DLO é descartada do DataFrame com aviso no
  log. A staging é recriada quando o conjunto de colunas muda; a tabela final
  só recebe `ALTER ADD COLUMN`, nunca `DROP`.
- **Deletes:** o `Account` tem `is_deleted` (mantido como BOOLEAN, o dbt
  filtra). O `WorkOrder` **não** tem essa coluna na DLO — exclusão na origem
  pode não aparecer na tabela.

## Dívida técnica

O motor (auth, extração paginada, janela, staging + MERGE, retry) foi
**copiado** de `rj_crm__salesforce_datacloud` em vez de promovido para
`src/prefect_rj_iplanrio/`. Foi uma decisão consciente para não mexer numa
pipeline em produção no mesmo PR.

Quando as duas estabilizarem, unificar em
`src/prefect_rj_iplanrio/salesforce_datacloud/` e apontar as duas. Diferenças
que precisam sobreviver à unificação:

- o MERGE do CRM casa `id + data_particao` (tabela de evento); aqui é só `id`
  (cadastro);
- o CRM usa schema estático (`utils/schemas.py`); aqui o schema vem da DLO.
