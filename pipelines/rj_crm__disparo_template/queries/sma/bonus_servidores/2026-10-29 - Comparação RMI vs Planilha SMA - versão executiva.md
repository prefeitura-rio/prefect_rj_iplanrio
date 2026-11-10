# Comparação de Bases — RMI vs Planilha SMA

WhatsApp · 14º salário — Dia do Servidor, MobiRio e Servidores 2º envio

Data        29 de outubro de 2026
Elaboração  IplanRio
Bases       RMI (pessoa_fisica) · Planilha SMA
Canal       WhatsApp, via Salesforce e Meta

---

## Campanhas analisadas

Três disparos realizados em 26 e 28 de outubro de 2026 para servidores municipais do Rio de Janeiro, com fontes de dados distintas: a base RMI constrói a lista a partir do cadastro unificado `pessoa_fisica`, com priorização de qualidade de telefone; a planilha SMA usa os telefones fornecidos pela Secretaria Municipal de Administração, sem priorização.

| Campanha | Template | Fonte | Data |
|---|---|---|---|
| Dia do Servidor (RMI) | `recado_dia_do_servidor_v3` | RMI — pessoa_fisica | 28/10/2026 |
| MobiRio (SMA) | `recado_mobirio_pref_rio_bonus_v1` | Planilha SMA | 26/10/2026 |
| Servidores 2º envio (SMA) | `recado_servidores_pref_rio_14` | Planilha SMA | 26/10/2026 |

---

## Métricas-destaque

**Mensagens entregues**

| | Dia do Servidor (RMI) | MobiRio (SMA) | Servidores 2º envio (SMA) |
|---|---|---|---|
| Disparos | **117.526** | **1.962** | **61.312** |
| Entregues | **99.994** | **1.698** | **50.034** |
| % entregue | **85,1%** dos 117.526 disparos | **86,5%** dos 1.962 disparos | **81,6%** dos 61.312 disparos |
| Lidos | **63.863** | **1.382** | **36.273** |
| % lido | **54,3%** dos disparos | **70,4%** dos disparos | **59,2%** dos disparos |

> **Nota sobre comparabilidade de entrega e leitura:** o disparo RMI ocorreu em 28/10, enquanto os disparos SMA ocorreram em 26/10 — mais de 48 horas antes. Os eventos de entrega e leitura continuam chegando ao longo do tempo: quanto mais recente o disparo, menor o acúmulo observado. As taxas de entrega e leitura do RMI provavelmente estão subcontadas em relação às da SMA por esse efeito de janela de observação, e tendem a crescer nas próximas horas.

---

## Funil comparativo

| Etapa / Métrica | Dia do Servidor (RMI) | MobiRio (SMA) | Servidores 2º envio (SMA) | Observação |
|---|---|---|---|---|
| Disparados (Log Salesforce) | 117.526 | 1.962 | 61.312 | Registros com evento de disparo confirmado |
| Números únicos | 117.090 | 1.915 | 61.058 | Deduplicação por telefone |
| CPFs únicos | 117.524 | 1.962 | 61.312 | Deduplicação por CPF |
| Erro Salesforce | 0 | 0 | 0 | Nenhuma falha antes de chegar à Meta |
| Erro Meta (total) | 15.870 — 13,5% | 208 — 10,6% | 10.559 — 17,2% | **Gargalo:** base SMA Servidores com maior taxa de falha |
| Erro Meta — número inválido | 11.956 — 75,3% dos erros | 151 — 72,6% dos erros | 9.017 — 85,4% dos erros | `Message Undeliverable`: número sem WhatsApp ou expirado |
| Erro Meta — experiment | 2.124 — 13,4% dos erros | 30 — 14,4% dos erros | 1.058 — 10,0% dos erros | `Phone number part of an experiment`: retenção interna da Meta |
| Erro Meta — healthy | 1.790 — 11,3% dos erros | 27 — 13,0% dos erros | 484 — 4,6% dos erros | `Healthy ecosystem engagement`: proteção contra baixo engajamento |
| Enviados à Meta | 101.233 | 1.712 | 50.501 | Com pelo menos um evento de envio, entrega ou leitura registrado |
| Entregues (dois tiques cinzas) | 99.994 — 85,1% | 1.698 — 86,5% | 50.034 — 81,6% | Evento OttDelivered observado |
| Lidos | 63.863 — 54,3% | 1.382 — 70,4% | 36.273 — 59,2% | Evento OttRead observado |

*Percentuais de erro por motivo calculados sobre o total de erros de cada campanha. Percentuais de entrega e leitura calculados sobre o total de disparos.*

---

## Erros da Meta — decomposição

Os erros da Meta foram classificados em três categorias mutuamente exclusivas:

**`Message Undeliverable` — número inválido ou sem WhatsApp**

Representa a maior parcela dos erros nas três campanhas. Na base Servidores SMA, chegou a 85,4% dos erros — 9 em cada 10 falhas ocorreram porque o número da planilha SMA não tinha WhatsApp ativo ou estava expirado. Na base RMI, essa proporção foi de 75,3%, indicando qualidade de cadastro superior, mas ainda com margem de melhoria.

| | Dia do Servidor (RMI) | MobiRio (SMA) | Servidores 2º envio (SMA) |
|---|---|---|---|
| `Message Undeliverable` | 11.956 | 151 | 9.017 |
| % dos erros | 75,3% | 72,6% | 85,4% |
| % dos disparos | 10,2% | 7,7% | 14,7% |

**`Phone number part of an experiment` — retenção interna da Meta**

Números retidos em experimentos de controle da Meta. Não relacionado à qualidade do cadastro — ação da plataforma.

| | Dia do Servidor (RMI) | MobiRio (SMA) | Servidores 2º envio (SMA) |
|---|---|---|---|
| `experiment` | 2.124 | 30 | 1.058 |
| % dos erros | 13,4% | 14,4% | 10,0% |
| % dos disparos | 1,8% | 1,5% | 1,7% |

**`Healthy ecosystem engagement` — proteção contra baixo engajamento**

Números bloqueados temporariamente pela Meta para preservar métricas do ecossistema. Relacionado ao comportamento histórico do usuário.

| | Dia do Servidor (RMI) | MobiRio (SMA) | Servidores 2º envio (SMA) |
|---|---|---|---|
| `healthy` | 1.790 | 27 | 484 |
| % dos erros | 11,3% | 13,0% | 4,6% |
| % dos disparos | 1,5% | 1,4% | 0,8% |

---

## Sobreposição entre campanhas

### CPFs únicos no conjunto das três campanhas

Do total de **122.062 CPFs únicos** disparados pelas três campanhas:

| Grupo | CPFs | % |
|---|---|---|
| Só RMI | 58.789 | 48,2% |
| RMI + Servidores | 58.669 | 48,1% |
| Só Servidores | 2.642 | 2,2% |
| Só MobiRio | 1.895 | 1,6% |
| RMI + MobiRio | 66 | 0,1% |
| MobiRio + Servidores | 1 | 0,0% |

Praticamente toda a base Servidores SMA (96,6% dos CPFs) também consta na base RMI. O disparo RMI e o disparo Servidores SMA alcançaram o mesmo universo de pessoas na grande maioria dos casos.

### Sobreposição por telefone — RMI × Servidores SMA

A análise por telefone (não por CPF) revela uma dimensão adicional: bases distintas podem usar números distintos para o mesmo CPF.

| Dimensão | Em comum | Só Servidores SMA |
|---|---|---|
| Por telefone | 51.408 | 9.650 |
| Por CPF | 58.669 | 2.643 |

A diferença entre as duas dimensões — 7.261 CPFs em ambas mas com telefones distintos — representa servidores para os quais o RMI priorizou um número diferente do que constava na planilha SMA.

### Resultado dos 51.408 telefones em ambas as campanhas

| Resultado de entrega | Resultado de leitura | Telefones | % |
|---|---|---|---|
| Ambos entregaram | Ambos lidos | 28.741 | 55,9% |
| Ambos entregaram | Nenhum lido | 12.168 | 23,7% |
| Nenhum entregou | Nenhum lido | 5.798 | 11,3% |
| Ambos entregaram | SMA lido, RMI não | 3.435 | 6,7% |
| SMA entregou, RMI não | SMA lido, RMI não | 707 | 1,4% |
| SMA entregou, RMI não | Nenhum lido | 353 | 0,7% |
| RMI entregou, SMA não | RMI lido, SMA não | 92 | 0,2% |
| RMI entregou, SMA não | Nenhum lido | 62 | 0,1% |
| Ambos entregaram | RMI lido, SMA não | 52 | 0,1% |

79,6% dos 51.408 telefones em comum tiveram entrega confirmada pelas duas campanhas. Em 1.060 casos (2,1%), a SMA entregou e o RMI não — o número da planilha SMA era válido mas o número priorizado pelo RMI não. Em 154 casos (0,3%), o RMI entregou e a SMA não.

### Os 9.650 telefones exclusivos da planilha SMA

| Situação do CPF | Telefones | CPFs | Entregue | % entregue | Lido | % lido | Falhou Meta | % falha |
|---|---|---|---|---|---|---|---|---|
| CPF também no RMI (tel diferente) | 7.052 | 7.210 | 2.955 | 41,9% | 2.171 | 30,8% | 4.125 | 58,5% |
| CPF exclusivo da SMA | 2.605 | 2.623 | 1.629 | 62,5% | 1.229 | 47,2% | 964 | 37,0% |

Os 7.052 telefones com CPF já disparado pelo RMI (em outro número) apresentam taxa de falha de 58,5% — consistente com a hipótese de que o RMI priorizou o número correto e o número da planilha SMA era alternativo ou desatualizado.

Os **2.605 telefones com CPF exclusivo da SMA** representam servidores que a base RMI não alcançaria — a planilha SMA trouxe contatos genuinamente novos para esse grupo. A taxa de falha é menor (37,0%) e a entrega atingiu 62,5%.

---

## Gargalos identificados

### 1. Qualidade do cadastro telefônico — planilha SMA

A base Servidores SMA apresentou taxa de falha por número inválido de **14,7% dos disparos** (9.017 casos com `Message Undeliverable`), contra **10,2%** na base RMI. A diferença de 4,5 pontos percentuais representa aproximadamente 2.760 mensagens adicionais que não chegaram ao destinatário por conta da qualidade inferior dos telefones na planilha SMA.

> 9.017 falhas por número inválido na base Servidores SMA — 85,4% de todos os erros dessa campanha

### 2. Servidores sem cadastro na base RMI

Dos 9.650 telefones exclusivos da SMA, 2.605 pertencem a CPFs que não constam na base RMI. Esses servidores só foram alcançados graças à planilha SMA. A ausência deles no cadastro unificado pode indicar servidores recém-admitidos, servidores de órgãos com integração incompleta ao RMI, ou divergências de CPF entre os sistemas.

> 2.623 CPFs alcançados exclusivamente pela planilha SMA, sem par na base RMI

---

## O que fica em aberto

A causa dos 2.643 CPFs exclusivos da SMA — servidores não cadastrados no RMI — também merece investigação junto à equipe do RMI (`@miloskimatheus`) para entender se são servidores sem integração ao sistema unificado ou casos de divergência de CPF entre a fonte da planilha SMA e o cadastro do RMI.
