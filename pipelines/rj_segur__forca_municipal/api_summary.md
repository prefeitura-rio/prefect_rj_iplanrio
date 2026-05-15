# HxGN.CIVITASAPI — Resumo Técnico (FM-RJ)

- **Versão do documento:** 1.1 (29/04/2025 — Integração Data Lake)
- **Sistema:** HxGN Oncall / CIVITAS — Força Municipal Rio de Janeiro

---

## Visão Geral

A HxGN.CIVITASAPI é uma camada intermediária que expõe dados do sistema CAD (Computer-Aided Dispatch) **HxGN CAD** da Hexagon para consumo externo. O cliente (nosso Prefect pipeline) autentica na CIVITASAPI, que por sua vez consulta o HxGN CAD e retorna os dados.

**Base URL:**
```
https://data.corio-oncall.com.br:8086/api
```

**Swagger:**
```
https://data.corio-oncall.com.br:8086/api/swagger
```

> **BLOQUEANTE — Acesso restrito à rede interna da Força Municipal.** Os pipelines Prefect precisarão rodar dentro dessa rede ou via VPN/túnel autorizado.

---

## Índice

- [Sumário de Endpoints](#sumário-de-endpoints)
- [Endpoints Prioritários para o Projeto CompStat](#endpoints-prioritários-para-o-projeto-compstat)
- [Observações Importantes](#observações-importantes)
- [Glossário de Domínio](#glossário-de-domínio)
- [Autenticação](#autenticação)
- [Paginação](#paginação)
- [Endpoints](#endpoints)
  - [1. POST /api/login](#1-post-apilogin) — autentica e retorna o Bearer Token JWT
  - [2. GET /api/unidades/ativas](#2-get-apiunidadesativas) — viaturas logadas com status, localização e ocorrência atribuída em tempo real
  - [3. GET /api/unidades/historico](#3-get-apiunidadeshistorico) — histórico completo de transições de status de todas as viaturas
  - [4. GET /api/ocorrencias/ativas](#4-get-apiocorrenciasativas) — ocorrências em curso (v1, sem geolocalização embutida)
  - [5. GET /api/ocorrencias/ativas/v2](#5-get-apiocorrenciasativasv2) — ocorrências em curso com lat/long e endereço; versão recomendada para o painel
  - [6. GET /api/ocorrencias/historico](#6-get-apiocorrenciashistorico) — todas as revisões de todas as ocorrências; uma linha por mudança de estado
  - [7. GET /api/qmd](#7-get-apiqmd) — catálogo completo de QMDs independente de status
  - [8. GET /api/qmd/ativos](#8-get-apiqmdativos) — somente QMDs com `StatusAtivo=true` no momento da consulta
  - [9. GET /api/qmd/{id}](#9-get-apiqmdid) — detalhamento de um QMD: missões, viaturas alocadas, execuções planejadas e geometria WKT
  - [10. GET /api/qmd/{id}/kml](#10-get-apiqmdidkml) — arquivo KML do QMD para visualização geográfica ad-hoc
  - [11. GET /api/qmd/servicos](#11-get-apiqmdservicos) — relação entre viaturas e QMDs com os dias de execução
  - [12. GET /api/qmd/missoes](#12-get-apiqmdmissoes) — missões de todos os QMDs sem geometria nem execuções detalhadas
  - [13. GET /api/qmd/plano](#13-get-apiqmdplano) — planos semanais de referência que agrupam os QMDs
  - [14. GET /api/unit/positions](#14-get-apiunitpositions) — rastro GPS histórico de uma viatura em um intervalo de data e hora

---

## Sumário de Endpoints

| # | Método | Endpoint | Descrição | Auth | Paginação | Parâmetros obrigatórios | Prioridade CompStat |
|---|--------|----------|-----------|------|-----------|-------------------------|---------------------|
| 1 | `POST` | [`/api/login`](#1-post-apilogin) | Autentica o usuário e retorna Bearer Token JWT com tempo de expiração | Não | Não | `userName`, `password` | — |
| 2 | `GET` | [`/api/unidades/ativas`](#2-get-apiunidadesativas) | Viaturas logadas no sistema com status operacional, posição GPS e ocorrência atribuída em tempo real | Sim | Sim | — | Alta |
| 3 | `GET` | [`/api/unidades/historico`](#3-get-apiunidadeshistorico) | Histórico de todas as transições de status das viaturas; uma linha por mudança de estado | Sim | Sim | — | Média |
| 4 | `GET` | [`/api/ocorrencias/ativas`](#4-get-apiocorrenciasativas) | Ocorrências em curso — v1, sem lat/long embutido; prefira o v2 para o painel | Sim | Sim | — | Baixa |
| 5 | `GET` | [`/api/ocorrencias/ativas/v2`](#5-get-apiocorrenciasativasv2) | Ocorrências em curso com lat/long, endereço e agentes; versão recomendada para o painel em tempo real | Sim | Não | — | Alta |
| 6 | `GET` | [`/api/ocorrencias/historico`](#6-get-apiocorrenciashistorico) | Todas as revisões de todas as ocorrências; uma linha por mudança; base principal para análise CompStat | Sim | Sim | — | Média |
| 7 | `GET` | [`/api/qmd`](#7-get-apiqmd) | Catálogo completo de QMDs (Quadros de Missão Diária), todos os status | Sim | Sim | — | Baixa |
| 8 | `GET` | [`/api/qmd/ativos`](#8-get-apiqmdativos) | Somente QMDs com `StatusAtivo=true`; planejamento operacional vigente | Sim | Sim | — | Alta |
| 9 | `GET` | [`/api/qmd/{id}`](#9-get-apiqmdid) | Detalhamento completo de um QMD: missões, viaturas, execuções planejadas e geometria WKT (`POINT`, `LINESTRING`, `POLYGON`) | Sim | Sim | `id` (path) | Média |
| 10 | `GET` | [`/api/qmd/{id}/kml`](#10-get-apiqmdidkml) | Arquivo KML binário de um QMD para visualização geográfica; não ingerir no BigQuery | Sim | Não | `id` (path) | Baixa |
| 11 | `GET` | [`/api/qmd/servicos`](#11-get-apiqmdservicos) | Relação entre viaturas e QMDs com os dias da semana de execução | Sim | Sim | — | Baixa |
| 12 | `GET` | [`/api/qmd/missoes`](#12-get-apiqmdmissoes) | Missões de todos os QMDs sem geometria nem execuções; versão resumida de `/qmd/{id}` | Sim | Sim | — | Baixa |
| 13 | `GET` | [`/api/qmd/plano`](#13-get-apiqmdplano) | Planos semanais de referência que agrupam os QMDs por período | Sim | Sim | — | Baixa |
| 14 | `GET` | [`/api/unit/positions`](#14-get-apiunitpositions) | Rastro GPS histórico de uma viatura em intervalo de data/hora; crítico para aderência ao QMD e heatmaps | Sim | Sim | `unitId`, `dataInicio`, `dataFim`, `horaInicio`, `horaFim` | Média |

---

## Endpoints Prioritários para o Projeto CompStat

### Alta prioridade — ingestão contínua (Prefect)

| Endpoint | Frequência sugerida | Uso no painel |
|----------|--------------------|----|
| `GET /api/unidades/ativas` | 1–5 min | Mapa em tempo real das viaturas |
| `GET /api/ocorrencias/ativas/v2` | 1–5 min | Mapa em tempo real das ocorrências com geolocalização |
| `GET /api/qmd/ativos` | 15–30 min | Planejamento vigente do dia |

### Média prioridade — ingestão histórica/diária

| Endpoint | Frequência sugerida | Uso |
|----------|--------------------|----|
| `GET /api/ocorrencias/historico` | Diário / incremental | Base histórica para análise CompStat |
| `GET /api/unidades/historico` | Diário / incremental | Análise de ocupação e disponibilidade de viaturas |
| `GET /api/unit/positions` | Diário por viatura | Aderência ao QMD, heatmaps de patrulhamento |
| `GET /api/qmd/{id}` | Sob demanda após QMD ativo | Planejamento completo com geometria WKT |

### Baixa prioridade — dados de apoio

| Endpoint | Uso |
|----------|-----|
| `GET /api/qmd` | Catálogo completo de QMDs |
| `GET /api/qmd/missoes` | Missões individuais sem geometria |
| `GET /api/qmd/servicos` | Relação viatura/QMD por dia da semana |
| `GET /api/qmd/plano` | Planos semanais de referência |
| `GET /api/qmd/{id}/kml` | Visualização geográfica ad-hoc — não ingerir no BigQuery |

---

## Observações Importantes

### Acesso à rede
> **BLOQUEANTE:** A API está acessível apenas na rede interna da Força Municipal. Os pipelines Prefect precisarão rodar dentro dessa rede ou via VPN/túnel autorizado. Alinhar com a TI da GM a solução de conectividade antes de iniciar o desenvolvimento dos pipelines.

### Token e expiração
- O campo `expirationTime` indica quando o token expira.
- Implementar renovação automática: fazer login novamente antes de cada execução do pipeline ou ao receber `401`.
- Armazenar credenciais em Secret/Vault — nunca no código ou no repositório.

### Paginação obrigatória
- Nenhum endpoint retorna todos os registros em uma única chamada.
- Usar sempre `pageSize=500` (máximo) e iterar por `totalPages` para ingestão completa.

### Qualidade dos dados geográficos
- `Latitude` e `Longitude` podem ser `0` em `/api/unidades/ativas` quando a viatura não tem GPS ativo — filtrar no modelo dbt.
- Em `/api/unit/positions`, pontos aberrantes aparecem nos dados de exemplo (ex: coordenada completamente fora da cidade). Implementar filtro de bounding box do Rio de Janeiro nos modelos dbt.
- A `geometriaWkt` do `/api/qmd/{id}` suporta `POINT`, `LINESTRING` e `POLYGON` — usar `ST_GeomFromText()` no BigQuery para converter para tipo `GEOGRAPHY`.

### Versionamento de ocorrências
- `/api/ocorrencias/historico` retorna **uma linha por revisão**, não uma por ocorrência. O campo `RevisionNumber` identifica a versão. Para o estado atual, filtrar pelo maior `RevisionNumber` por `AgencyEventId`.
- O mesmo padrão ocorre em `/api/unidades/historico` — cada transição de status gera um registro com `ActionCode`.

### Inconsistência de tipos
- `Status` em `/api/unidades/ativas` é `int`, mas em `/api/unidades/historico` é `string` (ex: `"DP"`, `"QE"`). Atenção ao definir o schema no BigQuery — usar `STRING` no histórico, `INT64` no ativas.

### CustomData
- O campo `CustomData` é serializado como JSON em string (ex: `{"Estado":"Informado"}`). Usar `JSON_EXTRACT_SCALAR()` no BigQuery para acessar os valores.

### Inconsistência de URL
- `/api/qmd/missoes`: o documento descreve o endpoint com 's' mas a URL de exemplo usa `/api/qmd/missao` (sem 's'). Testar ambas na integração.

---

## Glossário de Domínio

| Sigla | Significado |
|-------|-------------|
| QMD | Quadro de Missão Diária |
| PB | Ponto de Baseamento |
| PTR | Patrulhamento |
| RF | Ronda a Pé (ou variante — confirmar com a GM) |
| DM | Desvio de Missão |
| AGD | Agente Desmobilizado |
| POG | Tipo de viatura da GM |
| VTR | Viatura |
| MOT | Motocicleta |
| DP | Status de despacho (Dispatched) |
| QE | Status de a caminho (En Route) |
| AISP | Área Integrada de Segurança Pública |
| Beat | Setor/subsetor de patrulhamento |
| Zone | Zona geográfica ampla (ex: Zona Sul, Centro) |
| Area | Área de planejamento (ex: `AP4.1`) |
| Esz | Emergency Service Zone — código de zona emergencial |
| AgencyId | Identificador da agência — sempre `"FORCA"` neste contexto |
| CAD | Computer-Aided Dispatch — sistema de despacho |

---

## Autenticação

Todos os endpoints (exceto `/api/login`) exigem Bearer Token no header:
```
Authorization: Bearer <accessToken>
```

O token tem expiração (`expirationTime`). Implementar renovação automática nos pipelines ao receber `401`.

---

## Paginação

A maioria dos endpoints de listagem aceita os parâmetros abaixo. A resposta sempre inclui os metadados de paginação.

| Parâmetro | Tipo | Padrão | Min | Max |
|-----------|------|--------|-----|-----|
| `page` | int | 1 | — | — |
| `pageSize` | int | 50 | 1 | 500 |

**Envelope de resposta padrão:**
```json
{
  "page": 1,
  "pageSize": 50,
  "totalItems": 100,
  "totalPages": 2,
  "data": [ ... ]
}
```

> Use sempre `pageSize=500` nos pipelines para minimizar o número de requisições.

---

## Endpoints

---

### 1. `POST /api/login`

Autentica o usuário e retorna um Bearer Token JWT para uso nos demais endpoints.

**Parâmetros (body JSON):**

| Campo | Tipo | Obrigatório | Descrição |
|-------|------|-------------|-----------|
| `userName` | string | sim | Nome do usuário |
| `password` | string | sim | Senha |

**Exemplo de request:**
```bash
curl -X POST https://data.corio-oncall.com.br:8086/api/login \
  -H "Content-Type: application/json" \
  -d '{"userName": "username", "password": "pw"}'
```

**Exemplo de resposta (200 OK):**
```json
{
  "accessToken": "abc123xyz890",
  "expirationTime": "2025-01-15T14:52:00Z"
}
```

**Códigos de erro:**

| Código | Descrição |
|--------|-----------|
| `400` | Body inválido |
| `401` | Credenciais incorretas |
| `423` | Usuário bloqueado |
| `500` | Erro interno |

---

### 2. `GET /api/unidades/ativas`

Retorna todas as viaturas atualmente logadas no sistema com seu status operacional em tempo real.

**Parâmetros de query:**

| Parâmetro | Tipo | Padrão | Descrição |
|-----------|------|--------|-----------|
| `page` | int | 1 | Página |
| `pageSize` | int | 50 | Itens por página (max 500) |

**Exemplo de request:**
```bash
curl -X GET "https://data.corio-oncall.com.br:8086/api/unidades/ativas?page=1&pageSize=500" \
  -H "Authorization: Bearer <token>"
```

**Exemplo de resposta (200 OK):**
```json
{
  "page": 1,
  "pageSize": 2,
  "totalItems": 2,
  "totalPages": 1,
  "data": [
    {
      "AgencyId": "FORCA",
      "AlarmTime": 0,
      "AssignedAgencyEventId": null,
      "Beat": "Leblon",
      "ChangeComment": null,
      "CustomData": null,
      "DefaultAvailableStatus": 4,
      "DelayTime": 0,
      "DispatchAlarmLevel": null,
      "DispatchGroup": "23 AISP - Leblon",
      "IsUnavailable": false,
      "Latitude": 0,
      "Location": null,
      "LogonTime": "2025-11-07T12:25:39-03:00",
      "Longitude": 0,
      "OutOfServiceTypeCode": null,
      "Status": 4,
      "StatusChangeTime": "2025-11-07T15:25:39Z",
      "StatusedAgencyEventId": null,
      "StatusedAgencyEventSubtypeCode": null,
      "StatusedAgencyEventTypeCode": null,
      "TotalEventTime": null,
      "TotalUnavailableTime": null,
      "UnitId": "POG01",
      "UnitType": "POG",
      "UpdateCount": 5,
      "Zone": "ZONA SUL"
    },
    {
      "AgencyId": "FORCA",
      "AlarmTime": 0,
      "AssignedAgencyEventId": "FORCA2025093000034",
      "Beat": "Leblon",
      "ChangeComment": null,
      "CustomData": null,
      "DefaultAvailableStatus": 0,
      "DelayTime": 0,
      "DispatchAlarmLevel": 0,
      "DispatchGroup": "23 AISP - Leblon",
      "IsUnavailable": false,
      "Latitude": -22.98633020014094,
      "Location": "Avenida Rio Branco, 43, Centro, Rio de Janeiro - RJ, 20090-002, Brasil",
      "LogonTime": "2025-09-30T14:53:32-03:00",
      "Longitude": -43.21801945567132,
      "OutOfServiceTypeCode": null,
      "Status": 7,
      "StatusChangeTime": "2025-11-07T13:10:12Z",
      "StatusedAgencyEventId": "FORCA2025093000034",
      "StatusedAgencyEventSubtypeCode": null,
      "StatusedAgencyEventTypeCode": "A",
      "TotalEventTime": null,
      "TotalUnavailableTime": 3266201,
      "UnitId": "POG02",
      "UnitType": "POG",
      "UpdateCount": 3,
      "Zone": "Zona Sul"
    }
  ]
}
```

**Dicionário de dados:**

| Campo | Tipo | Descrição |
|-------|------|-----------|
| `UnitId` | string | Identificador da viatura (ex: `POG01`) |
| `UnitType` | string | Tipo da viatura (`POG`, `VTR`, `MOT`) |
| `AgencyId` | string | Agência responsável (sempre `FORCA`) |
| `Status` | int | Status atual da unidade |
| `Latitude` / `Longitude` | float | Posição atual (pode ser `0` se GPS inativo) |
| `Location` | string | Endereço por extenso |
| `Beat` | string | Setor de patrulhamento (ex: `Leblon`) |
| `Zone` | string | Zona geográfica ampla (ex: `Zona Sul`) |
| `DispatchGroup` | string | Grupo de despacho (ex: `23 AISP - Leblon`) |
| `AssignedAgencyEventId` | string | ID da ocorrência atribuída à viatura |
| `LogonTime` | string | Momento de login da viatura no sistema |
| `StatusChangeTime` | string | Timestamp do último update de status |
| `IsUnavailable` | boolean | Se a unidade está indisponível |
| `TotalUnavailableTime` | int | Tempo total indisponível em ms |
| `TotalEventTime` | int | Tempo total em ocorrências em ms |
| `DefaultAvailableStatus` | int | Status padrão de disponibilidade |
| `AlarmTime` / `DelayTime` | int | Tempos de alarme e atraso em ms |
| `DispatchAlarmLevel` | int | Nível de alarme de despacho |
| `OutOfServiceTypeCode` | string | Código do motivo de saída de serviço |
| `StatusedAgencyEventId` | string | ID da ocorrência vinculada ao status atual |
| `StatusedAgencyEventTypeCode` | string | Tipo da ocorrência vinculada |
| `UpdateCount` | int | Contador de atualizações da viatura |

**Erros:** `401` token ausente ou inválido | `500` erro inesperado

---

### 3. `GET /api/unidades/historico`

Retorna o histórico completo de todas as mudanças de estado das viaturas. Cada linha representa uma transição de status.

**Parâmetros de query:**

| Parâmetro | Tipo | Padrão | Descrição |
|-----------|------|--------|-----------|
| `page` | int | 1 | Página |
| `pageSize` | int | 50 | Itens por página (max 500) |

**Exemplo de request:**
```bash
curl -X GET "https://data.corio-oncall.com.br:8086/api/unidades/historico?page=1&pageSize=500" \
  -H "Authorization: Bearer <token>"
```

**Exemplo de resposta (200 OK):**
```json
{
  "page": 1,
  "pageSize": 2,
  "totalItems": 2,
  "totalPages": 1,
  "data": [
    {
      "ActionCode": 29,
      "ActiveUnitHistoryId": "1E17F629-95BA-42E2-B640-2D8281604E3C",
      "AgencyId": "FORCA",
      "AlarmTime": 0,
      "AreEmployeesTracked": false,
      "AssignedAgencyEventId": "FORCA2025093000034",
      "Beat": "Botafogo",
      "ChangeComment": null,
      "CreatedEmployeeId": 8034342,
      "CreatedTime": "2025-10-30T19:40:37Z",
      "CustomData": null,
      "DatabaseInsertTime": "2025-10-30T16:40:37-03:00",
      "DefaultAvailableStatus": 0,
      "DelayTime": 0,
      "DispatchAlarmLevel": 0,
      "DispatchGroup": "2 AISP - Botafogo",
      "IsUnavailable": false,
      "Latitude": 0,
      "LineupName": "LIENUP_FORCA",
      "Location": "Avenida Rio Branco, 43, Centro, Rio de Janeiro - RJ, 20090-002, Brasil",
      "LogonTime": "2025-09-30T15:02:31-03:00",
      "Longitude": 0,
      "OrderWithinCreatedTime": 2461,
      "OutOfServiceTypeCode": null,
      "StationId": null,
      "Status": "DP",
      "StatusedAgencyEventId": "FORCA2025093000034",
      "StatusedAgencyEventRevisionNum": 4,
      "StatusedAgencyEventSubtypeCode": null,
      "StatusedAgencyEventTypeCode": "A",
      "TotalAvailableStationTime": null,
      "TotalEventCount": null,
      "TotalEventTime": 1907574,
      "TotalUnavailableTime": 2597891,
      "UnitId": "POG08",
      "UnitType": "POG",
      "Zone": "Zona Sul"
    }
  ]
}
```

**Dicionário de dados (campos adicionais em relação ao `/ativas`):**

| Campo | Tipo | Descrição |
|-------|------|-----------|
| `ActiveUnitHistoryId` | string | UUID único do registro de histórico |
| `ActionCode` | int | Código da ação/transição de status |
| `Status` | string | Status em formato textual (ex: `DP`, `QE`) — diferente do `/ativas` que é int |
| `CreatedTime` | string | Timestamp da criação do registro |
| `DatabaseInsertTime` | string | Timestamp de inserção no banco |
| `LineupName` | string | Nome da escala (ex: `LIENUP_FORCA`) |
| `OrderWithinCreatedTime` | int | Ordem de criação dentro do mesmo timestamp |
| `StatusedAgencyEventRevisionNum` | int | Revisão da ocorrência em que a viatura atuou |
| `TotalEventCount` | int | Total de ocorrências atendidas |
| `TotalAvailableStationTime` | string | Tempo total disponível na base |
| `AreEmployeesTracked` | boolean | Se funcionários estão sendo rastreados |
| `StationId` | string | ID da estação/base vinculada |
| `CreatedEmployeeId` | int | ID do funcionário que gerou o registro |

**Erros:** `401` token ausente ou inválido | `500` erro inesperado

---

### 4. `GET /api/ocorrencias/ativas`

Retorna as ocorrências em curso no sistema (versão 1, sem geolocalização embutida).

> Prefira `/api/ocorrencias/ativas/v2` para o painel — inclui lat/long diretamente.

**Parâmetros de query:**

| Parâmetro | Tipo | Padrão | Descrição |
|-----------|------|--------|-----------|
| `page` | int | 1 | Página |
| `pageSize` | int | 50 | Itens por página (max 500) |

**Exemplo de request:**
```bash
curl -X GET "https://data.corio-oncall.com.br:8086/api/ocorrencias/ativas?page=1&pageSize=500" \
  -H "Authorization: Bearer <token>"
```

**Exemplo de resposta (200 OK):**
```json
{
  "page": 1,
  "pageSize": 1,
  "totalItems": 3,
  "totalPages": 3,
  "data": [
    {
      "AddedTime": "2025-09-30T18:21:09Z",
      "AgencyEventId": "FORCA2025093000001",
      "AgencyEventSubtypeCode": "default",
      "AgencyEventSubtypeCodeDesc": null,
      "AgencyEventTypeCode": "F",
      "AgencyEventTypeCodeDesc": "Furto",
      "AgencyId": "FORCA",
      "AlarmLevel": 0,
      "Area": "AP4.1",
      "Attributes": 72,
      "Beat": "Jacarepaguá",
      "ClosingComment": null,
      "ClosingEmployeeId": 12345,
      "ClosingTime": "2025-10-22T21:40:39Z",
      "CreatedEmployeeId": 12346,
      "CreatedTime": "2025-09-30T18:21:10Z",
      "CustomData": "{\"Estado\":\"Informado\"}",
      "DatabaseInsertTime": "2025-09-30T15:21:11-03:00",
      "DatabaseUpdateTime": "2025-10-22T18:40:40-03:00",
      "DispatchGroup": "18 AISP - Jacarepaguá",
      "District": "Jacarepaguá",
      "Esz": 21018115,
      "EventDescription": null,
      "FirstUnitArrivedTime": null,
      "FirstUnitDispatchedTime": "2025-10-22T21:40:30Z",
      "FirstUnitEnroutedTime": null,
      "IsOpen": false,
      "IsReopened": false,
      "LastStatusChangeTime": "2025-10-22T21:40:30Z",
      "Municipality": "Rio de Janeiro",
      "PendingAlarmTime": "2025-10-22T21:41:39Z",
      "PrimaryEmployeeId": null,
      "PrimaryUnitId": null,
      "Priority": 2,
      "PriorityChangedTime": null,
      "RevisionEmployeeId": 12345,
      "RevisionNumber": 5,
      "RevisionTime": "2025-10-22T21:40:39Z",
      "StartedTime": "2025-09-30T18:20:36Z",
      "StatusCode": 16,
      "SubstatusCode": 0,
      "TotalAssignedUnits": 0,
      "UpdatedEmployeeId": 12345,
      "UpdatedTime": "2025-10-22T21:40:39Z",
      "UserDefinedSupplementalInfo": 0,
      "UserGroupId": 10036,
      "Zone": "Zona Sudoeste"
    }
  ]
}
```

**Dicionário de dados:**

| Campo | Tipo | Descrição |
|-------|------|-----------|
| `AgencyEventId` | string | ID único da ocorrência (ex: `FORCA2025093000001`) |
| `AgencyEventTypeCode` | string | Código do tipo (ex: `F`=Furto, `R`=Roubo, `DM`=Desvio de Missão) |
| `AgencyEventTypeCodeDesc` | string | Descrição textual do tipo |
| `AgencyEventSubtypeCode` | string | Subtipo da ocorrência |
| `Area` | string | Área de planejamento (ex: `AP4.1`) |
| `Beat` | string | Setor de patrulhamento |
| `District` | string | Bairro da ocorrência |
| `Zone` | string | Zona geográfica |
| `Municipality` | string | Município |
| `DispatchGroup` | string | AISP responsável (ex: `18 AISP - Jacarepaguá`) |
| `Priority` | int | Prioridade da ocorrência |
| `StatusCode` | int | Código numérico de status |
| `SubstatusCode` | int | Sub-status |
| `IsOpen` | boolean | Se a ocorrência está aberta |
| `IsReopened` | boolean | Se foi reaberta |
| `AddedTime` | string | Inserção no sistema |
| `StartedTime` | string | Início da ocorrência |
| `CreatedTime` | string | Criação do registro |
| `ClosingTime` | string | Encerramento |
| `FirstUnitDispatchedTime` | string | Primeiro despacho de viatura |
| `FirstUnitArrivedTime` | string | Primeira chegada de viatura |
| `FirstUnitEnroutedTime` | string | Primeiro roteamento de viatura |
| `TotalAssignedUnits` | int | Total de viaturas despachadas |
| `Esz` | int | Código da Zona de Serviço Emergencial |
| `RevisionNumber` | int | Número da revisão do registro |
| `CustomData` | string | JSON livre (ex: `{"Estado":"Informado"}`) |

**Erros:** `401` token ausente ou inválido | `500` erro inesperado

---

### 5. `GET /api/ocorrencias/ativas/v2`

Versão enriquecida das ocorrências em curso. Inclui lat/long e localização diretamente no objeto. **Versão recomendada para o painel.**

> Diferente do v1: não utiliza paginação por parâmetros de query explícitos e inclui campos geográficos.

**Parâmetros de query:** nenhum além da autenticação.

**Exemplo de request:**
```bash
curl -X GET "https://data.corio-oncall.com.br:8086/api/ocorrencias/ativas/v2" \
  -H "Authorization: Bearer <token>"
```

**Exemplo de resposta (200 OK):**
```json
{
  "data": [
    {
      "CreatedTime": "2026-04-26T00:15:11+00:00",
      "AgencyEventId": "FORCA2026010201876",
      "AgencyEventSubtypeCode": "DM",
      "AgencyEventSubtypeCodeDesc": null,
      "AgencyEventTypeCode": "DM",
      "AgencyEventTypeCodeDesc": "Desvio de missão",
      "Area": "AP1.1",
      "Attributes": 0,
      "Beat": "Centro",
      "ClosingComment": null,
      "ClosingEmployeeId": null,
      "ClosingTime": null,
      "CreatedEmployeeId": 3,
      "CustomData": "{\"Estado\":\"Informado\"}",
      "DatabaseUpdateTime": "2026-04-25T21:15:11-03:00",
      "DispatchGroup": "23 AISP - Leblon",
      "District": "Centro",
      "Esz": 21005005,
      "EventDescription": "Desvio de missão",
      "FirstUnitArrivedTime": null,
      "FirstUnitDispatchedTime": null,
      "FirstUnitEnroutedTime": null,
      "LastStatusChangeTime": "2026-04-26T00:15:11+00:00",
      "PrimaryEmployeeId": null,
      "PrimaryUnitId": null,
      "Priority": 7,
      "PriorityChangedTime": null,
      "StatusCode": 7,
      "UserGroupId": 14,
      "Zone": "Centro",
      "Latitude": -22.9026,
      "Longitude": -43.1742,
      "Location": "Rua Ulisses Guimarães, Estácio, Rio de Janeiro - RJ, 20211-225, Brasil",
      "Agents": null
    }
  ]
}
```

**Campos adicionais em relação ao v1:**

| Campo | Tipo | Descrição |
|-------|------|-----------|
| `Latitude` / `Longitude` | float | Coordenadas geográficas da ocorrência |
| `Location` | string | Endereço por extenso |
| `Agents` | string | Lista de agentes envolvidos (pode ser `null`) |

**Erros:** `401` token ausente ou inválido | `500` erro inesperado

---

### 6. `GET /api/ocorrencias/historico`

Retorna todas as alterações de todas as ocorrências — uma linha por revisão. Útil para reconstruir a linha do tempo de cada evento.

**Parâmetros de query:**

| Parâmetro | Tipo | Padrão | Descrição |
|-----------|------|--------|-----------|
| `page` | int | 1 | Página |
| `pageSize` | int | 50 | Itens por página (max 500) |

**Exemplo de request:**
```bash
curl -X GET "https://data.corio-oncall.com.br:8086/api/ocorrencias/historico?page=1&pageSize=500" \
  -H "Authorization: Bearer <token>"
```

**Exemplo de resposta (200 OK):**
```json
{
  "page": 1,
  "pageSize": 2,
  "totalItems": 2,
  "totalPages": 1,
  "data": [
    {
      "AddedTime": "2025-09-30T18:21:09Z",
      "AgencyEventId": "FORCA2025093000001",
      "AgencyEventSubtypeCode": "default",
      "AgencyEventSubtypeCodeDesc": null,
      "AgencyEventTypeCode": "F",
      "AgencyEventTypeCodeDesc": "Furto",
      "AgencyId": "FORCA",
      "AlarmLevel": 0,
      "Area": "AP4.1",
      "Attributes": 64,
      "Beat": "Jacarepaguá",
      "ClosingComment": null,
      "ClosingEmployeeId": null,
      "ClosingTime": null,
      "CreatedEmployeeId": 12345,
      "CreatedTime": "2025-10-22T21:40:30Z",
      "CustomData": "{\"Estado\":\"Informado\"}",
      "DatabaseInsertTime": "2025-10-22T18:40:31-03:00",
      "DispatchGroup": "18 AISP - Jacarepaguá",
      "District": "Jacarepaguá",
      "Esz": 21018115,
      "EventDescription": null,
      "IsOpen": true,
      "IsReopened": false,
      "LastStatusChangeTime": "2025-09-30T18:21:09Z",
      "Municipality": "Rio de Janeiro",
      "OriginalEmployeeId": 12346,
      "PendingAlarmTime": "2025-09-30T18:21:09Z",
      "Priority": 2,
      "RevisionEmployeeId": 12346,
      "RevisionNumber": 1,
      "RevisionTime": "2025-09-30T18:21:09Z",
      "StartedTime": "2025-09-30T18:20:36Z",
      "StatusCode": 7,
      "SubstatusCode": 0,
      "TotalAssignedUnits": 0,
      "UserDefinedSupplementalInfo": 0,
      "UserGroupId": 14,
      "Zone": "Zona Sudoeste"
    }
  ]
}
```

**Campo adicional em relação ao `/ativas`:**

| Campo | Tipo | Descrição |
|-------|------|-----------|
| `OriginalEmployeeId` | int | ID do funcionário que iniciou a ocorrência originalmente |

> A mesma `AgencyEventId` aparece múltiplas vezes com `RevisionNumber` crescente. Para obter o estado atual de uma ocorrência, filtrar pelo maior `RevisionNumber`.

**Erros:** `401` token ausente ou inválido | `500` erro inesperado

---

### 7. `GET /api/qmd`

Retorna todos os QMDs (Quadros de Missão Diária) cadastrados no sistema, independente do status.

**Parâmetros de query:**

| Parâmetro | Tipo | Padrão | Descrição |
|-----------|------|--------|-----------|
| `page` | int | 1 | Página |
| `pageSize` | int | 50 | Itens por página (max 500) |

**Exemplo de request:**
```bash
curl -X GET "https://data.corio-oncall.com.br:8086/api/qmd?page=1&pageSize=500" \
  -H "Authorization: Bearer <token>"
```

**Exemplo de resposta (200 OK):**
```json
{
  "page": 1,
  "pageSize": 1,
  "totalItems": 1,
  "totalPages": 1,
  "data": [
    {
      "Id": 1,
      "Nome": "Largo da Carioca - Turno A",
      "Area": "Centro",
      "Resumo": "teste",
      "Prescricoes": "teste",
      "DataVigenciaInicio": "2025-10-26T03:00:00Z",
      "DataVigenciaFim": "2025-11-23T03:00:00Z",
      "HoraExecucaoInicio": "16:00",
      "HoraExecucaoFim": "22:00",
      "StatusAtivo": true,
      "StatusValido": true,
      "StatusAutorizado": true,
      "IdRespCriacao": 210003,
      "IdRespAutorizacao": 210003,
      "DataHoraCriacao": "2025-10-31T15:18:20.1406974Z",
      "DataHoraAutorizacao": "2025-10-31T18:55:56.3906235Z"
    }
  ]
}
```

**Dicionário de dados:**

| Campo | Tipo | Descrição |
|-------|------|-----------|
| `Id` | int | Identificador do QMD |
| `Nome` | string | Nome descritivo (ex: `Largo da Carioca - Turno A`) |
| `Area` | string | Área de atuação |
| `Resumo` | string | Resumo do objetivo |
| `Prescricoes` | string | Diretrizes táticas para a guarnição |
| `DataVigenciaInicio` / `DataVigenciaFim` | string | Período de validade |
| `HoraExecucaoInicio` / `HoraExecucaoFim` | string | Janela diária de execução |
| `StatusAtivo` | boolean | QMD está ativo |
| `StatusValido` | boolean | QMD está dentro da vigência |
| `StatusAutorizado` | boolean | QMD foi autorizado |
| `IdRespCriacao` / `IdRespAutorizacao` | int | IDs dos funcionários responsáveis |
| `DataHoraCriacao` / `DataHoraAutorizacao` | string | Timestamps de criação e autorização |

**Erros:** `401` token ausente ou inválido | `500` erro inesperado

---

### 8. `GET /api/qmd/ativos`

Retorna apenas os QMDs com `StatusAtivo=true` no momento da consulta. Mesma estrutura do `/api/qmd`.

**Parâmetros de query:**

| Parâmetro | Tipo | Padrão | Descrição |
|-----------|------|--------|-----------|
| `page` | int | 1 | Página |
| `pageSize` | int | 50 | Itens por página (max 500) |

**Exemplo de request:**
```bash
curl -X GET "https://data.corio-oncall.com.br:8086/api/qmd/ativos?page=1&pageSize=500" \
  -H "Authorization: Bearer <token>"
```

**Exemplo de resposta (200 OK):**
```json
{
  "page": 1,
  "pageSize": 50,
  "totalItems": 1,
  "totalPages": 1,
  "data": [
    {
      "Id": 49,
      "Nome": "Academia da Força Municipal - Tarde (5ª edição)",
      "Area": "Academia da Força Municipal",
      "Resumo": "Realizar patrulhamento cumprindo roteiro e ponto de baseamento descritos no quadro em anexo com objetivo de proteger as instalações e o perímetro do Centro de Treinamento.",
      "Prescricoes": "A guarnição deverá agir em caráter preventivo, obedecendo às técnicas pertinentes para cada objetivo da operação, conforme briefing executado, de maneira proativa, com o pessoal, material e meios adequados.",
      "DataVigenciaInicio": "2026-04-08T00:00:00-03:00",
      "DataVigenciaFim": "2026-04-11T23:59:59-03:00",
      "HoraExecucaoInicio": "06:00",
      "HoraExecucaoFim": "22:00",
      "StatusAtivo": true,
      "StatusValido": true,
      "StatusAutorizado": true,
      "IdRespCriacao": 123,
      "IdRespAutorizacao": 123,
      "DataHoraCriacao": "2026-04-08T13:10:00.6478355-03:00",
      "DataHoraAutorizacao": "2026-04-08T13:10:08.9075377-03:00"
    }
  ]
}
```

**Erros:** `401` token ausente ou inválido | `500` erro inesperado

---

### 9. `GET /api/qmd/{id}`

Retorna o detalhamento completo de um QMD específico, incluindo missões com geometria geográfica (WKT), serviços (viaturas) alocadas e execuções planejadas por data.

**Parâmetros de path:**

| Parâmetro | Tipo | Obrigatório | Descrição |
|-----------|------|-------------|-----------|
| `id` | int | sim | ID do QMD |

**Parâmetros de query:**

| Parâmetro | Tipo | Padrão | Descrição |
|-----------|------|--------|-----------|
| `page` | int | 1 | Página |
| `pageSize` | int | 50 | Itens por página (max 500) |

**Exemplo de request:**
```bash
curl -X GET "https://data.corio-oncall.com.br:8086/api/qmd/49" \
  -H "Authorization: Bearer <token>"
```

**Exemplo de resposta (200 OK):**
```json
{
  "qmdId": 49,
  "qmdNome": "Academia da Força Municipal - Tarde (5ª edição)",
  "qmdResumo": "Realizar patrulhamento cumprindo roteiro e ponto de baseamento descritos no quadro em anexo.",
  "qmdArea": "Academia da Força Municipal",
  "qmdDataVigenciaInicio": "2026-04-08T00:00:00-03:00",
  "qmdDataVigenciaFim": "2026-04-11T23:59:59-03:00",
  "qmdStatusAtivo": "Sim",
  "qmdStatusAutorizado": "Sim",
  "qmdStatusValido": "Sim",
  "missoes": [
    {
      "missaoId": 273,
      "tipo": "PB",
      "roteiro": "Hangar de Operações aéreas",
      "horaInicio": "06:00",
      "horaFim": "08:00",
      "geometriaWkt": "POINT (-43.325822 -22.818465)",
      "servicos": [
        {
          "servicoId": 415,
          "nome": "POG10",
          "dias": "[\"seg\",\"ter\",\"qua\",\"qui\",\"sex\"]",
          "execucoes": [
            {
              "dataHoraInicio": "2026-04-06T06:00:00-03:00",
              "dataHoraFim": "2026-04-06T08:00:00-03:00"
            },
            {
              "dataHoraInicio": "2026-04-07T06:00:00-03:00",
              "dataHoraFim": "2026-04-07T08:00:00-03:00"
            }
          ]
        }
      ]
    },
    {
      "missaoId": 274,
      "tipo": "PTR",
      "roteiro": "Interior da Academia",
      "horaInicio": "08:00",
      "horaFim": "09:00",
      "geometriaWkt": "LINESTRING (-43.325976 -22.818317, -43.327296 -22.819691, -43.328379 -22.818218)",
      "servicos": [ ]
    },
    {
      "missaoId": 275,
      "tipo": "RF",
      "roteiro": "Refeitório da Academia",
      "horaInicio": "09:00",
      "horaFim": "10:00",
      "geometriaWkt": "POLYGON ((-43.326868 -22.815736, -43.325597 -22.816408, -43.327153 -22.818548, -43.328344 -22.81787, -43.326868 -22.815736))",
      "servicos": [ ]
    }
  ]
}
```

**Dicionário de dados:**

| Campo | Tipo | Descrição |
|-------|------|-----------|
| `qmdId` | int | ID do QMD |
| `qmdNome` | string | Nome completo |
| `qmdResumo` | string | Resumo do objetivo |
| `qmdArea` | string | Área de atuação |
| `qmdDataVigenciaInicio` / `qmdDataVigenciaFim` | string | Período de vigência |
| `qmdStatusAtivo` / `qmdStatusAutorizado` / `qmdStatusValido` | string | Flags em formato textual `"Sim"/"Não"` |
| `missoes[].missaoId` | int | ID da missão |
| `missoes[].tipo` | string | Tipo: `PB`, `PTR` ou `RF` |
| `missoes[].roteiro` | string | Descrição textual do roteiro |
| `missoes[].horaInicio` / `horaFim` | string | Janela de execução |
| `missoes[].geometriaWkt` | string | Geometria em WKT (`POINT`, `LINESTRING` ou `POLYGON`) |
| `missoes[].servicos[].nome` | string | ID da viatura alocada (ex: `POG10`) |
| `missoes[].servicos[].dias` | array[string] | Dias de execução |
| `missoes[].servicos[].execucoes[]` | array | Datas/horas de execução planejadas |

**Tipos de missão:**

| Sigla | Descrição |
|-------|-----------|
| `PB` | Ponto de Baseamento — geometria do tipo `POINT` |
| `PTR` | Patrulhamento — geometria do tipo `LINESTRING` |
| `RF` | Ronda a Pé — geometria do tipo `POLYGON` |

**Erros:** `401` token ausente ou inválido | `500` erro inesperado

---

### 10. `GET /api/qmd/{id}/kml`

Retorna o arquivo KML geográfico de um QMD específico para visualização em ferramentas como Google Earth.

**Parâmetros de path:**

| Parâmetro | Tipo | Obrigatório | Descrição |
|-----------|------|-------------|-----------|
| `id` | int | sim | ID do QMD |

**Exemplo de request:**
```bash
curl -X GET "https://data.corio-oncall.com.br:8086/api/qmd/10/kml" \
  -H "Authorization: Bearer <token>" \
  -o qmd_10.kml
```

**Exemplo de response headers (200 OK):**
```
content-disposition: attachment; filename="qmd_10_R do Senado - Diurno.kml"
content-type: application/vnd.google-earth.kml+xml
content-length: 9521
```

**Body:** arquivo KML binário.

> Baixa prioridade para pipeline de dados. Útil apenas para visualização geográfica ad-hoc.

**Erros:** `401` token ausente ou inválido | `500` erro inesperado

---

### 11. `GET /api/qmd/servicos`

Retorna todos os serviços dos QMDs — a relação entre viaturas e QMDs com os dias de execução.

**Parâmetros de query:**

| Parâmetro | Tipo | Padrão | Descrição |
|-----------|------|--------|-----------|
| `page` | int | 1 | Página |
| `pageSize` | int | 50 | Itens por página (max 500) |

**Exemplo de request:**
```bash
curl -X GET "https://data.corio-oncall.com.br:8086/api/qmd/servicos?page=1&pageSize=500" \
  -H "Authorization: Bearer <token>"
```

**Exemplo de resposta (200 OK):**
```json
{
  "page": 1,
  "pageSize": 1,
  "totalItems": 1,
  "totalPages": 1,
  "data": [
    {
      "Id": 1,
      "IdPlano": 1,
      "IdQmd": 1,
      "Nome": "VTR01",
      "Dias": "[\"dom\",\"seg\"]"
    }
  ]
}
```

**Dicionário de dados:**

| Campo | Tipo | Descrição |
|-------|------|-----------|
| `Id` | int | ID do serviço |
| `IdPlano` | int | ID do plano de referência |
| `IdQmd` | int | ID do QMD pai |
| `Nome` | string | ID da viatura alocada (ex: `POG01`, `VTR01`) |
| `Dias` | string (JSON) | Dias de execução serializados (ex: `["dom","seg"]`) |

**Erros:** `401` token ausente ou inválido | `500` erro inesperado

---

### 12. `GET /api/qmd/missoes`

Retorna todas as missões de todos os QMDs do sistema, sem os detalhes de geometria e execuções.

> **Atenção:** o documento apresenta inconsistência — a seção descreve `/api/qmd/missoes` mas a URL de exemplo usa `/api/qmd/missao` (sem 's'). Testar ambas na integração.

**Parâmetros de query:**

| Parâmetro | Tipo | Padrão | Descrição |
|-----------|------|--------|-----------|
| `page` | int | 1 | Página |
| `pageSize` | int | 50 | Itens por página (max 500) |

**Exemplo de request:**
```bash
curl -X GET "https://data.corio-oncall.com.br:8086/api/qmd/missoes?page=1&pageSize=500" \
  -H "Authorization: Bearer <token>"
```

**Exemplo de resposta (200 OK):**
```json
{
  "page": 1,
  "pageSize": 3,
  "totalItems": 3,
  "totalPages": 1,
  "data": [
    {
      "Id": 2,
      "IdQmd": 1,
      "TipoMissao": "PB",
      "Roteiro": "Largo da Carioca",
      "HoraInicio": "16:00",
      "HoraFim": "22:00"
    },
    {
      "Id": 3,
      "IdQmd": 1,
      "TipoMissao": "PTR",
      "Roteiro": "R. do Mercado, R. do Ouvidor, R. Primeiro de Março, Av. Erasmo Braga, R. Dom Manuel, Terminal de Barcas",
      "HoraInicio": "18:30",
      "HoraFim": "20:00"
    },
    {
      "Id": 4,
      "IdQmd": 1,
      "TipoMissao": "RF",
      "Roteiro": "Centro",
      "HoraInicio": "20:00",
      "HoraFim": "21:00"
    }
  ]
}
```

**Dicionário de dados:**

| Campo | Tipo | Descrição |
|-------|------|-----------|
| `Id` | int | ID da missão |
| `IdQmd` | int | ID do QMD pai |
| `TipoMissao` | string | Tipo: `PB`, `PTR`, `RF` |
| `Roteiro` | string | Descrição textual do roteiro |
| `HoraInicio` / `HoraFim` | string | Janela de execução |

**Erros:** `401` token ausente ou inválido | `500` erro inesperado

---

### 13. `GET /api/qmd/plano`

Retorna todos os planos semanais de referência dos QMDs.

**Parâmetros de query:**

| Parâmetro | Tipo | Padrão | Descrição |
|-----------|------|--------|-----------|
| `page` | int | 1 | Página |
| `pageSize` | int | 50 | Itens por página (max 500) |

**Exemplo de request:**
```bash
curl -X GET "https://data.corio-oncall.com.br:8086/api/qmd/plano?page=1&pageSize=500" \
  -H "Authorization: Bearer <token>"
```

**Exemplo de resposta (200 OK):**
```json
{
  "page": 1,
  "pageSize": 1,
  "totalItems": 1,
  "totalPages": 1,
  "data": [
    {
      "Id": 1,
      "Nome": "Plano Piloto - Centro 2025",
      "SemanaReferenciaInicio": "2025-10-26T03:00:00Z",
      "SemanaReferenciaFim": "2025-11-02T02:59:59.9990000Z",
      "IdRespCriacao": 210003,
      "DataHoraCriacao": "2025-10-31T23:46:49.2029481Z"
    }
  ]
}
```

**Dicionário de dados:**

| Campo | Tipo | Descrição |
|-------|------|-----------|
| `Id` | int | ID do plano |
| `Nome` | string | Nome descritivo (ex: `Plano Piloto - Centro 2025`) |
| `SemanaReferenciaInicio` / `SemanaReferenciaFim` | string | Semana de referência do plano |
| `IdRespCriacao` | int | ID do funcionário responsável |
| `DataHoraCriacao` | string | Timestamp de criação |

**Erros:** `401` token ausente ou inválido | `500` erro inesperado

---

### 14. `GET /api/unit/positions`

Retorna o rastro GPS histórico de uma viatura em um intervalo de data/hora. Endpoint crítico para análise de aderência ao QMD planejado.

**Parâmetros de query:**

| Parâmetro | Tipo | Obrigatório | Descrição |
|-----------|------|-------------|-----------|
| `unitId` | string | sim | ID da viatura (ex: `pog01`) |
| `dataInicio` | string | sim | Data de início (`YYYY-MM-DD`) |
| `dataFim` | string | sim | Data de fim (`YYYY-MM-DD`) |
| `horaInicio` | string | sim | Hora de início (`HH:MM`) |
| `horaFim` | string | sim | Hora de fim (`HH:MM`) |
| `page` | int | não | Página (padrão: 1) |
| `pageSize` | int | não | Itens por página, max 500 (padrão: 50) |

**Exemplo de request:**
```bash
curl -X GET "https://data.corio-oncall.com.br:8086/api/unit/positions?unitId=pog01&dataInicio=2026-04-14&dataFim=2026-04-14&horaInicio=12:00&horaFim=15:00&pageSize=500" \
  -H "Authorization: Bearer <token>"
```

**Exemplo de resposta (200 OK):**
```json
{
  "unitId": "pog01",
  "dataInicio": "2026-04-14",
  "dataFim": "2026-04-14",
  "horaInicio": "12:00",
  "horaFim": "15:00",
  "page": 1,
  "pageSize": 50,
  "totalItems": 17,
  "totalPages": 1,
  "data": [
    {
      "UnitId": "POG01",
      "Latitude": -22.9121533,
      "Longitude": -43.2033945,
      "Date": "2026-04-14T15:00:04+00:00"
    },
    {
      "UnitId": "POG01",
      "Latitude": -22.9121114,
      "Longitude": -43.2033686,
      "Date": "2026-04-14T15:03:06+00:00"
    },
    {
      "UnitId": "POG01",
      "Latitude": -22.9121594,
      "Longitude": -43.2033727,
      "Date": "2026-04-14T15:03:37+00:00"
    }
  ]
}
```

**Dicionário de dados:**

| Campo | Tipo | Descrição |
|-------|------|-----------|
| `UnitId` | string | ID da viatura |
| `Latitude` / `Longitude` | float | Coordenadas do ponto GPS |
| `Date` | string | Timestamp do registro com timezone |

**Erros:** `401` token ausente ou inválido | `500` erro inesperado


