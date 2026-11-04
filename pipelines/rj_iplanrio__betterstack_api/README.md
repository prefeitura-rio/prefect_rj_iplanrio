# Pipeline BetterStack API (rj_iplanrio__betterstack_api)

Este repositório contém o código da pipeline Prefect responsável por extrair dados de incidentes da BetterStack API e carregá-los no BigQuery da prefeitura.

## Objetivo
Registrar incidentes de interrupção de serviços de forma simplificada, permitindo a análise histórica e o acompanhamento de estabilidade dos serviços monitorados.

## Estrutura da Pipeline

- **`flow.py`**: Orquestrador do processo de ETL, focado na ingestão de incidentes.
- **`tasks.py`**: A task que busca os incidentes (API v3), página por página.
- **`helpers.py`**: Datas, paginação e as linhas do resultado.
- **`constants.py`**: A URL da API, o fuso e o tamanho da página.
- **`settings.py`**: `BETTERSTACK_TOKEN` e `BETTERSTACK_MONITOR_ID`.

## Dados Ingeridos

| Tabela | Origem (API) | Descrição |
| :--- | :--- | :--- |
| `eai_gateway_incidents` | `/v3/incidents` | Registro detalhado de incidentes. |

## Configurações e Deploy

A pipeline está configurada no `prefect.yaml` com dois deployments:

1.  **Staging**: Destino projeto `rj-iplanrio`.
2.  **Production**: Destino projeto `rj-iplanrio`. Agendamento diário às 03:00 AM (`cron: "0 3 * * *"`).

### Parâmetros do Flow
O flow recebe `cloud` e `table` do `prefect.yaml` e aceita os seguintes parâmetros opcionais para execuções manuais ou backfills:
- `from_date`: Data de início (formato `YYYY-MM-DD`). Se nulo, assume D-1.
- `to_date`: Data de fim (formato `YYYY-MM-DD`). Se nulo, assume D-1.

> [!NOTE]
> A pipeline utiliza o modo `append` para acumular dados históricos na tabela de incidentes.

### Requisitos
- **Token de API**: Deve estar configurado no Infisical (injetado via variáveis de ambiente) com a chave `BETTERSTACK_TOKEN`.
- **Monitor ID**: Deve estar configurado no Infisical com a chave `BETTERSTACK_MONITOR_ID`.

## Resiliência e Monitoramento
- **Retries**: As tarefas de busca de dados possuem 3 tentativas automáticas com 60s de intervalo.
- **Tabela**: Uma tabela `Delta` no bucket, lida pelo BigQuery por uma conexão BigLake. Cada coluna é texto, e `data_particao` (YYYY-MM-DD) é o dia do incidente.

---
*Documentação atualizada em Janeiro/2026.*
