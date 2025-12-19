# Pipeline BetterStack API (rj_iplanrio__betterstack_api)

Este repositório contém o código da pipeline Prefect responsável por extrair dados de monitoramento e incidentes da BetterStack API e carregá-los no BigQuery da prefeitura.

## Objetivo
Monitorar o tempo de resposta (uptime) e registrar incidentes de interrupção de serviços, permitindo a análise histórica e o acompanhamento de SLAs.

## Estrutura da Pipeline

- **`flow.py`**: Orquestrador do processo de ETL.
- **`tasks.py`**: Funções específicas de extração e transformação.
- **`constants.py`**: Constantes e configurações (URLs, Monitor ID, etc.).
- **`utils/tasks.py`**: Funções auxiliares de particionamento de dados.

## Dados Ingeridos

| Tabela | Origem (API) | Descrição |
| :--- | :--- | :--- |
| `eai_gateway_response_times` | `/v2/monitors/{id}/response-times` | Latência por região (Frequência Horária). |
| `eai_gateway_incidents` | `/v3/incidents` | Registro detalhado de incidentes. |
| `eai_gateway_daily_sla` | `/v2/monitors/{id}/sla` | Resumo diário de disponibilidade e KPIs. |

## Configurações e Deploy

A pipeline está configurada no `prefect.yaml` com dois deployments, ambos apontando para o projeto principal:

1.  **Staging**: Destino projeto `rj-iplanrio`.
2.  **Production**: Destino projeto `rj-iplanrio`. Agendamento horário (`cron: "0 * * * *"`).

> [!NOTE]
> Seguindo o padrão de simplificação, ambos os ambientes utilizam o projeto `rj-iplanrio` para faturamento e armazenamento, garantindo a existência do bucket de staging.

### Requisitos
- **Token de API**: Deve estar configurado no Infisical (injetado via variáveis de ambiente) com a chave `BETTERSTACK_TOKEN`.
- **Monitor ID**: Deve estar configurado no Infisical com a chave `MONITOR_ID`.

## Resiliência e Monitoramento
- **Retries**: As tarefas de busca de dados na API possuem 3 tentativas automáticas com 60s de intervalo.
- **Fail-Fast**: A pipeline quebra totalmente caso qualquer uma das tabelas falhe na ingestão, garantindo visibilidade total sobre falhas de dados.
- **Particionamento**: Utiliza o modelo BigLake particionado por `data_particao` (YYYY-MM-DD) em formato Parquet.

---
*Documentação atualizada em Dezembro/2025.*
