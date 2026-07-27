# View `despesa_classificada`

**Projeto:** `rj-nf-agent`  
**Dataset:** `brutos_cgm_poc_osinfo_ia_pipeline`  
**Objeto:** `rj-nf-agent.brutos_cgm_poc_osinfo_ia_pipeline.despesa_classificada`  
**Tipo:** VIEW (auto-atualiza a cada consulta)  
**Arquivo SQL:** `pipelines/rj_iplanrio__nf_agent/despesa_classificada.sql`

---

## O que é

Esta view classifica as declarações de despesa do OSInfo que tiveram seu PDF processado pela pipeline de extração de NFs. Para cada declaração, a view verifica se a nota fiscal mencionada foi encontrada no PDF, confere seus dados contra o que a IA extraiu, e consulta fontes externas (Receita Federal e SMF) para indicadores adicionais de conformidade.

A view substitui a view legada `rj-nf-agent.poc_osinfo_ia.vw_despesas_classificada`, que operava sobre um conjunto estático de dados. A nova view opera exclusivamente sobre documentos processados pela pipeline Prefect e se atualiza automaticamente conforme novos PDFs são processados — sem necessidade de recarga manual.

---

## Escopo

Apenas declarações cujo campo `descricao_limpa` (ou `descricao`) aponta para um PDF que já passou pela pipeline de extração (`extracao_pagina`). O join de escopo é feito por `nome_arquivo_limpo = extracao_pagina.nome_arquivo`.

Declarações cujo PDF ainda não foi processado simplesmente não aparecem na view.

---

## Fontes de dados

| Fonte | Tabela | Uso |
|---|---|---|
| OSInfo (declarações) | `rj-nf-agent.poc_osinfo_ia.osinfo_despesas_recorte` | Dados da declaração (valor, CNPJ, datas, arquivo) |
| Pipeline de extração | `rj-nf-agent.brutos_cgm_poc_osinfo_ia_pipeline.extracao_pagina` | Dados extraídos pela IA de cada página do PDF |
| Receita Federal (CNPJ) | `basedosdados.br_me_cnpj.estabelecimentos` + `.dicionario` | Situação cadastral do CNPJ do emitente |
| Recorte de cadastro | `rj-nf-agent.poc_osinfo_ia.bcadastro_cnpj_recorte` | Data de abertura do CNPJ |
| SMF — NFs canceladas | `rj-nf-agent.poc_osinfo_ia.smf_nf_cancelada_substituta_20260311` | Notas fiscais canceladas ou substituídas |

---

## Arquitetura interna (CTEs)

A view é construída em 13 CTEs antes do SELECT final.

| CTE | Descrição |
|---|---|
| `despesas` | Lê e normaliza todas as declarações do OSInfo (CNPJ, nome do arquivo) |
| `documentos_processados` | Filtra apenas as declarações cujo PDF está em `extracao_pagina` (INNER JOIN) |
| `match_proprio` | Páginas da `extracao_pagina` onde o `match_id_documento` bate com o `id_documento` da declaração **e** o `nome_arquivo` é o mesmo PDF referenciado pela declaração |
| `match_outro_pdf` | Páginas onde o `match_id_documento` bate com a declaração, mas o PDF é **diferente** do referenciado |
| `nf_extraida_por_arquivo` | PDFs que têm ao menos uma NF extraída com sucesso (tipo de documento válido) |
| `arquivos_com_erro` | PDFs com `pipeline_status = 'erro_processamento'` |
| `declaracoes_com_extracao` | Join principal: cruza declarações com resultados de extração e flags de contexto (`tem_match_outro_pdf`, `pdf_tem_nf_extraida`, `pdf_tem_erro_processamento`) |
| `base_cnpj_limpa` | Situação cadastral de CNPJs na Receita Federal, filtrada a partir de 2021-01-01 |
| `bcadastro` | Data de abertura dos CNPJs (recorte local) |
| `smf_nf_cancelada` | NFs canceladas/substituídas segundo o SMF |
| `metricas_por_nota` | Quantidade de declarações e soma dos valores pagos por nota fiscal (chave: número NF + CNPJ emitente) |
| `rank_por_nota` | Ranking das declarações dentro de cada nota (ordem: data_envio, id_documento) |
| `indicadores` | Computa os 7 indicadores booleanos e os metadados para o SELECT final |

---

## Indicadores

Todos os indicadores são expostos como `ARRAY<STRUCT<nome STRING, valor BOOL, motivo STRING>>` na coluna `indicadores`. Cada indicador segue a semântica:

- **`TRUE`** — condição verificada, sem apontamento
- **`FALSE`** — condição violada, apontamento identificado
- **`NULL`** — não foi possível avaliar (dado ausente ou fora de escopo)

### Indicadores internos (derivados da própria extração)

#### `nf_encontrada`
Verifica se a NF declarada foi encontrada **no PDF referenciado pela declaração**.

- `TRUE`: a pipeline identificou um match entre a declaração e uma NF extraída do mesmo PDF
- `FALSE`: não encontrou correspondência no PDF correto

> **Importante:** mesmo que a NF apareça em outro PDF, o indicador é `FALSE`. O critério é exclusivamente o PDF mencionado na declaração. O campo `motivo_nf_nao_encontrada` detalha o sub-caso.

| `motivo_nf_nao_encontrada` | Significado |
|---|---|
| `pdf_processado_sem_match` | PDF processado, NFs extraídas, mas nenhuma casou com a declaração |
| `pdf_sem_nf_extraida` | PDF processado, mas nenhuma NF foi extraída (pode ser contrato, boleto etc.) |
| `match_em_pdf_errado` | A NF casou com um PDF diferente do referenciado pela declaração |

---

#### `match_valor`
Compara o valor declarado no OSInfo com o valor extraído pela IA do PDF.

- `TRUE`: valores iguais
- `FALSE`: valores diferentes
- `NULL`: declaração sem match, ou algum dos valores é nulo

Tolerância: comparação exata (sem margem). Gera **Apontamento Leve** quando `FALSE`.

---

#### `match_numero_documento`
Compara o número de documento declarado com o número extraído pela IA (apenas dígitos).

- `TRUE`: números iguais após normalização
- `FALSE`: números diferentes
- `NULL`: declaração sem match, ou algum dos campos é nulo

Indicador informativo — não entra na classificação final diretamente.

---

#### `match_cnpj`
Compara o CNPJ/CPF declarado no OSInfo com o CNPJ emitente extraído pela IA (14 dígitos normalizados).

- `TRUE`: CNPJs iguais
- `FALSE`: CNPJs diferentes
- `NULL`: declaração sem match, ou algum dos campos é nulo

Indicador informativo — não entra na classificação final diretamente.

---

#### `match_data_emissao`
Compara a data de emissão declarada com a data extraída pela IA. A data da IA pode vir em formato `YYYY-MM-DD` ou `DD/MM/YYYY`.

- `TRUE`: datas iguais
- `FALSE`: datas diferentes
- `NULL`: declaração sem match, ou algum dos campos é nulo

Indicador informativo — não entra na classificação final diretamente.

---

#### `nf_nao_duplicada`
Detecta rateio indevido: a mesma NF sendo paga por mais de uma unidade organizacional diferente, com a soma dos valores pagos excedendo o valor da NF em mais de R$ 1,00.

- `TRUE`: sem duplicidade detectada
- `FALSE`: NF duplicada (soma paga > valor NF + R$1, rank > 1, unidades diferentes)
- `NULL`: declaração sem match, ou dados insuficientes

Gera **Apontamento Grave** quando `FALSE`.

---

#### `valor_pago_menor_documento`
Verifica se a soma de todos os valores pagos referentes à mesma NF não ultrapassa o valor do documento em mais de R$ 1,00.

- `TRUE`: soma paga ≤ valor NF + R$1 (tolerância para centavos)
- `FALSE`: soma paga excede o valor da NF
- `NULL`: declaração sem match, ou dados insuficientes

Gera **Apontamento Grave** quando `FALSE`.

---

### Indicadores externos (fontes externas)

A semântica de `NULL` é uniforme para todos: **`NULL` significa que não foi possível encontrar a informação na fonte externa, não que a condição está ok.**

#### `cnpj_ativo`
Consulta `basedosdados.br_me_cnpj.estabelecimentos` para verificar se o CNPJ do emitente estava ativo na data do pagamento.

- `TRUE`: CNPJ com situação cadastral `'Ativa'` na data do pagamento
- `FALSE`: CNPJ existente na base, mas com outra situação cadastral na data do pagamento
- `NULL`: declaração sem match; CNPJ ou data de pagamento ausentes; pagamento anterior a 2021-11-01; ou CNPJ não encontrado na base

> O corte de `2021-11-01` existe porque a tabela do basedosdados é filtrada a partir de `2021-01-01`. Para pagamentos muito anteriores, os dados históricos podem não cobrir o período, então optamos por não avaliar.

Join: `cnpj_emitente_normalizado = e.cnpj AND data_pagamento >= e.data_situacao_cadastral`, com `QUALIFY ROW_NUMBER() ... ORDER BY data_situacao_cadastral DESC = 1` para pegar o registro mais recente antes do pagamento.

Gera **Apontamento Grave** quando `FALSE`.

---

#### `emissao_posterior_abertura_cnpj`
Consulta `bcadastro_cnpj_recorte` para verificar se a NF foi emitida após (ou na mesma data de) abertura do CNPJ.

- `TRUE`: data de emissão da NF ≥ data de abertura do CNPJ
- `FALSE`: data de emissão anterior à abertura do CNPJ
- `NULL`: declaração sem match; data de emissão ausente; ou CNPJ não encontrado no recorte local

> Quando o CNPJ não está no `bcadastro_cnpj_recorte`, o indicador é `NULL` — não é possível avaliar, não é um apontamento.

Join: `cnpj_cpf_normalizado = cleaned_cnpj_reg` (CNPJ do declarante, 14 dígitos).

Gera **Apontamento Grave** quando `FALSE`.

---

#### `nf_nao_cancelada`
Consulta a tabela SMF de NFs canceladas/substituídas.

- `TRUE`: NF encontrada na base SMF, sem `data_cancelamento` (nunca foi cancelada)
- `FALSE`: NF encontrada na base SMF, com cancelamento sem substituta declarada
- `NULL`: NF não encontrada na base SMF (não conseguimos avaliar); ou declaração sem match

Join: `nome_arquivo_limpo = nome_arquivo_declaracao AND numero_documento_ia (dígitos) = numero_nf_modelo AND cnpj_emitente_ia = cnpj_cpf_modelo`.

> A ausência da NF na tabela SMF **não implica que ela não foi cancelada** — significa apenas que não temos essa informação. Por isso o resultado é `NULL`, não `TRUE`.

Gera **Apontamento Grave** quando `FALSE`.

---

## Classificação final

A coluna `classificacao_modelo` agrega todos os indicadores em uma única categoria:

| Classificação | Condição |
|---|---|
| `Não avaliado — erro de processamento` | PDF com `pipeline_status = 'erro_processamento'` |
| `Sem match` | `nf_encontrada = FALSE` e motivo = `pdf_processado_sem_match` |
| `Apontamento Grave` | `nf_encontrada = FALSE` (outros motivos), ou `nf_encontrada = TRUE` com qualquer indicador grave `= FALSE` |
| `Apontamento Leve` | `nf_encontrada = TRUE`, nenhum indicador grave disparado, mas `match_valor = FALSE` |
| `Sem apontamentos` | `nf_encontrada = TRUE`, todos os indicadores avaliados sem problemas |

**Indicadores que geram Grave:** `valor_pago_menor_documento`, `nf_nao_duplicada`, `cnpj_ativo`, `emissao_posterior_abertura_cnpj`, `nf_nao_cancelada`.

> Indicadores com `NULL` não geram apontamento — apenas indicadores explicitamente `FALSE` disparam classificação negativa.

---

## Distribuição atual (referência)

Dados do conjunto de 43 PDFs processados até o momento da documentação:

| Classificação | Qtd |
|---|---|
| Sem match | 636 |
| Apontamento Grave | 58 |
| Sem apontamentos | 20 |
| **Total** | **714** |

Detalhamento dos 58 Graves:
- 26 por `pdf_sem_nf_extraida` (PDF não continha NF extraível)
- 31 por `cnpj_ativo = FALSE` (CNPJ inativo na data do pagamento)
- 1 por `match_valor = FALSE` após encontrar a NF (valor divergente — declarado R$88.420, extraído R$8.420)

---

## Colunas da view

| Coluna | Tipo | Descrição |
|---|---|---|
| `id_documento` | INT64 | Chave da declaração no OSInfo |
| `cod_organizacao` | STRING | Código da organização |
| `cod_unidade` | STRING | Código da unidade |
| `data_envio_declaracao` | DATE | Data de envio da declaração |
| `nome_arquivo_declaracao` | STRING | Nome do PDF referenciado (sem extensão) |
| `referencia_ano_declaracao` | INT64 | Ano de referência |
| `referencia_mes_declaracao` | INT64 | Mês de referência |
| `numero_documento_declaracao` | STRING | Número do documento declarado |
| `cnpj_cpf_declaracao` | STRING | CNPJ/CPF declarado (formatado) |
| `valor_documento_declaracao` | NUMERIC | Valor do documento declarado |
| `valor_pago_declaracao` | NUMERIC | Valor pago declarado |
| `data_emissao_declaracao` | DATE | Data de emissão declarada |
| `data_pagamento_declaracao` | DATE | Data de pagamento declarada |
| `pagina_match` | INT64 | Página do PDF onde foi encontrada a NF (NULL se sem match) |
| `nome_arquivo_match` | STRING | Nome do PDF onde ocorreu o match (deve ser igual ao declarado) |
| `tipo_documento_classificacao_ia` | STRING | Classificação do tipo de documento pela IA |
| `tipo_documento_extracao_ia` | STRING | Tipo de documento identificado na extração |
| `numero_documento_ia` | STRING | Número da NF extraído pela IA |
| `cnpj_emitente_ia` | STRING | CNPJ do emitente extraído pela IA |
| `cnpj_destinatario_ia` | STRING | CNPJ do destinatário extraído pela IA |
| `valor_documento_ia` | FLOAT64 | Valor do documento extraído pela IA |
| `data_emissao_documento_ia` | STRING | Data de emissão extraída pela IA (string, formato variável) |
| `data_competencia_documento_ia` | STRING | Data de competência extraída pela IA |
| `data_servico_documento_ia` | STRING | Data de serviço extraída pela IA |
| `numero_rps_ia` | STRING | Número RPS extraído pela IA |
| `observacao_extracao_ia` | STRING | Observações da extração |
| `qtd_declaracoes_mesma_nota` | INT64 | Quantas declarações referenciam a mesma NF (por número + CNPJ) |
| `soma_valor_pago_mesma_nota` | NUMERIC | Soma dos valores pagos para a mesma NF |
| `rank_declaracao` | INT64 | Posição desta declaração entre as que referenciam a mesma NF |
| `indicadores` | ARRAY<STRUCT<nome, valor, motivo>> | Array com os 7 indicadores booleanos |
| `classificacao_modelo` | STRING | Classificação final da declaração |
| `motivo_nf_nao_encontrada` | STRING | Sub-motivo quando `nf_encontrada = FALSE` |
| `timestamp_geracao_pagina` | TIMESTAMP | Timestamp de quando a página foi processada pela pipeline |
| `versao_pipeline` | STRING | Versão da pipeline que gerou a extração |
| `timestamp_consulta` | TIMESTAMP | Momento em que a view foi consultada |

---

## Decisões de design

**VIEW, não TABLE**  
A view se atualiza automaticamente conforme novos PDFs são processados pela pipeline Prefect. Não é necessário nenhum script de recarga ou agendamento adicional.

**Escopo restrito à pipeline**  
O INNER JOIN com `extracao_pagina` garante que apenas declarações com PDF efetivamente processado entram na view. Isso reduz o universo de ~1,2M de declarações no OSInfo para o conjunto relevante (714 no momento).

**`nf_encontrada` é estrito ao PDF correto**  
Um match em outro PDF não conta. Mesmo que a NF exista em algum outro documento processado, o indicador será `FALSE` se ela não estiver no PDF que a declaração referencia. O sub-motivo `match_em_pdf_errado` identifica esse caso.

**Indicadores externos retornam NULL quando sem dados**  
A ausência de um CNPJ na base da Receita, ou de uma NF na tabela SMF, resulta em `NULL` — não em `TRUE`. Assumir que está ok por não estar em uma lista de irregularidades seria um falso positivo.

**Corte temporal para `cnpj_ativo`**  
Pagamentos anteriores a `2021-11-01` retornam `NULL` para este indicador. A tabela do basedosdados é filtrada a partir de `2021-01-01`, e usar dados de situação cadastral de 2021 para avaliar um pagamento de 2018, por exemplo, seria impreciso.

**QUALIFY para `cnpj_ativo`**  
Como o join com `base_cnpj_limpa` pode retornar múltiplas linhas por documento (diferentes datas de mudança de situação cadastral), o `QUALIFY ROW_NUMBER() OVER (PARTITION BY id_documento ORDER BY data_situacao_cadastral DESC) = 1` garante que apenas o registro de situação mais recente antes da data de pagamento é considerado.

---

## Histórico de mudanças

### v2 (atual)

- **Conversão de TABLE para VIEW**: eliminada a necessidade de recarga manual; a view reflete sempre o estado atual da pipeline.
- **Escopo restrito**: adicionado INNER JOIN com `extracao_pagina` (CTE `documentos_processados`), reduzindo o universo de ~1,2M para apenas as declarações com PDF processado.
- **Indicadores como array de structs**: coluna `indicadores` substituiu colunas booleanas individuais; campo `pode_ser_null` removido da struct.
- **Três indicadores externos adicionados:**
  - `cnpj_ativo` — via `basedosdados.br_me_cnpj.estabelecimentos`
  - `emissao_posterior_abertura_cnpj` — via `bcadastro_cnpj_recorte`
  - `nf_nao_cancelada` — via `smf_nf_cancelada_substituta_20260311`
- **`nf_encontrada` restrito ao PDF correto**: match em PDF diferente do referenciado pela declaração resulta em `FALSE`, com `motivo_nf_nao_encontrada = 'match_em_pdf_errado'`.
- **Semântica NULL corrigida em `emissao_posterior_abertura_cnpj`**: quando o CNPJ não é encontrado no `bcadastro_cnpj_recorte`, o indicador retorna `NULL` (não avaliado) em vez de `FALSE` (apontamento indevido).
- **Semântica NULL uniforme para indicadores externos**: `NULL` significa ausência de dado, nunca conformidade implícita.
