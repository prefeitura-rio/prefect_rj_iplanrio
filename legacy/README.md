# legacy

Pipelines que ainda não migraram para a estrutura nova (`src/utils`, `toolkit`,
`pipelines/<nome>`). O código é o do `master` como estava no merge e nenhum check,
teste ou deploy do repositório olha esta pasta.

| Pipeline | Motivo |
|---|---|
| `rj_crm__relatorio_engajamento_hsm` | RAG com torch e Drive/Sheets, a migração ficou para o time de CRM |
| `rj_smfp__nota_carioca_extracao_xml` | webservice SOAP e leitura de `NFSE_OWNER.TAREFAS` no Oracle, novo no `master` em 29/10, precisa dos segredos novos no Infisical |

Os deployments de prod continuam os que já estavam publicados. A `carga_completa` chama o deployment da extração XML pelo nome. Para voltar a publicar, migre a
pipeline para `pipelines/` e inclua o nome em `.allow.json`.
