# legacy

Pipelines que ainda não migraram para a estrutura nova (`src/utils`, `toolkit`,
`pipelines/<nome>`). O código é o do `master` como estava no merge e nenhum check,
teste ou deploy do repositório olha esta pasta.

| Pipeline | Motivo |
|---|---|
| `rj_crm__relatorio_engajamento_hsm` | RAG com torch e Drive/Sheets, a migração ficou para o time de CRM |

O deployment de prod continua o que já estava publicado. Para voltar a publicar, migre a
pipeline para `pipelines/` e inclua o nome em `.allow.json`.
