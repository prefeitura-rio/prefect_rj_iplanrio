# Relatório de URLs redigidas (`rj_crm__whitelist_whatsapp_relatorio`)

Relata diariamente as URLs que o agente de IA do Salesforce redigiu nas conversas, para
que sejam avaliadas e liberadas na whitelist do WhatsApp. O destinatário do e-mail é o
censor, que repassa os endereços por cópia a quem de fato altera a whitelist — por isso o
corpo traz um bloco monoespaçado pronto para copiar, com a contagem no título.

Não tem relação técnica com a pipeline irmã `rj_crm__whitelist_whatsapp`: outra fonte,
outro ciclo de vida.

## O que entrega

| Canal | Conteúdo | Quando |
| --- | --- | --- |
| Discord | resumo curto: janela, contagem e endereços distintos | **todo dia**, com ou sem ocorrência |
| E-mail | relatório detalhado: bloco de cópia, frequência e linha a linha | **só quando há ocorrência** |

A entrega diária no Discord é proposital: num relatório recorrente o silêncio é ambíguo,
não distingue "sem dados" de "não rodou".

## Quando roda

Diariamente às **09:00** (`America/Sao_Paulo`), inclusive fins de semana e feriados.
A janela é `[ontem 09:00, hoje 09:00)` — semiaberta, para que o fim de uma seja o início
da próxima, sem lacuna nem repetição.

Só o deployment de produção tem agendamento; o de staging roda sob demanda.

## Fonte

`rj-crm-registry.brutos_salesforce.ai_agent_interaction_step` — passos em que
`saida_valor_texto` contém `URL_Redacted` **e** o atributo `redacted_urls` está
preenchido. Passo sem o atributo está fora de escopo: não é acionável.

## Variáveis de ambiente

Todas obrigatórias, validadas na primeira task. Faltando qualquer uma, o flow falha
antes de qualquer envio, nomeando o que falta.

| Variável | Conteúdo |
| --- | --- |
| `DISCORD_WEBHOOK_URL_WHITELIST_WHATSAPP_RELATORIO` | webhook do canal do relatório |
| `CRM_WHITELIST_WHATSAPP_RELATORIO_DATA_RELAY_URL` | endpoint `/data/mailman` do Data Relay |
| `CRM_WHITELIST_WHATSAPP_RELATORIO_DATA_RELAY_API_KEY` | chave do header `x-api-key`, escopada **apenas** a `/data/mailman` |
| `CRM_WHITELIST_WHATSAPP_RELATORIO_DATA_RELAY_TO_ADDRESSES` | destinatários do e-mail, separados por vírgula |

Cadastrar no conjunto do Infisical que sincroniza para
`prefect-jobs-crm-registry-secrets` (prod) e `prefect-jobs-crm-registry-secrets-staging`.
As `BASEDOSDADOS_*` já existem nos dois conjuntos e não precisam de ação.

Os dois ambientes usam o mesmo canal do Discord; a linha `Ambiente:` do cabeçalho é que
distingue as mensagens. Para separar, basta apontar outro webhook no secret de staging.

## Reprocessar uma janela

`start_datetime` e `end_datetime`, no formato `YYYY-MM-DD HH:MM:SS`, informados **juntos**
(um sem o outro é erro). Omitidos, a janela vem do horário agendado.

Reexecutar reenvia o relatório daquela janela — nada é gravado, então não há estado a
corrigir, e rodar duas vezes manda duas mensagens.

Execução atrasada não perde dados: a janela é ancorada no horário agendado, então uma
execução que ficou `Late` calcula exatamente o recorte que lhe cabia.

## Custo

**912 MB por execução**, mais **733 MB apenas nos dias sem ocorrência**, quando a guarda
consulta a última ocorrência conhecida. Algo em torno de **US$ 4 por ano**.

A tabela não é particionada nem clusterizada: no BigQuery a cobrança é por bytes de
coluna varridos, então um filtro por data reduziria o resultado sem reduzir o custo. Por
isso a query não tem um — e não vale pedir particionamento ao time dono da tabela.

## Como ler o canal

| Sinal | Significado |
| --- | --- |
| nenhuma mensagem no dia | o flow não rodou, ou falhou antes de publicar |
| ⚠️ **O envio do e-mail falhou** | houve ocorrência, mas o Data Relay não entregou; as URLs estão na execução do Prefect |
| "última ocorrência conhecida há N dias" crescendo | a fonte parou de gerar ocorrências — investigar se o marcador `URL_Redacted` mudou ou a redação foi desligada |
