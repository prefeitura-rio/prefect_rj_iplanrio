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
| `response_times` | `/v2/monitors/{id}/response-times` | Latência por região geográfica. |
| `incidents` | `/v3/incidents` | Registro de incidentes ocorridos no período. |

## Configurações e Deploy

A pipeline está configurada no `prefect.yaml` com dois deployments:

1.  **Staging**: Destino projeto `rj-iplanrio-dev`.
2.  **Production**: Destino projeto `rj-iplanrio`. Agendamento diário (`cron: "0 6 * * *"`).

### Requisitos
- **Token de API**: Deve estar configurado no Infisical no caminho `/api-betterstack` com a chave `betterstack_token`.

## Resiliência e Monitoramento
- **Retries**: As tarefas de busca de dados na API possuem 3 tentativas automáticas com 60s de intervalo.
- **Fail-Fast**: A pipeline quebra totalmente caso qualquer uma das tabelas falhe na ingestão, garantindo visibilidade total sobre falhas de dados.
- **Particionamento**: Utiliza o modelo BigLake particionado por `data_particao` (YYYY-MM-DD).

---
*Documentação gerada automaticamente para o repositório rj-iplanrio.*
