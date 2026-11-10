# Ata — Apresentação dos Resultados do Teste A/B de Cobrança de Multas (SMTR)

Data        29 de outubro de 2026
Participantes  Patrícia Catandi, Rodolpho Souza, Maria Luiza Wuillaume, Thiago (SMTR)
Elaboração  IplanRio

---

## Contexto

Apresentação dos resultados do primeiro teste A/B de cobrança de multas de trânsito via WhatsApp,
conduzido em parceria com a SMTR. A base foi construída a partir da tabela
`rj-smtr.transito.autuacao`, randomizada em 1.200 tratamento e 1.200 controle com seed `17041993`.
A metodologia aplicada foi **nudge**.

---

## Perguntas feitas na reunião

### Respondidas

| Pergunta | Resposta |
|---|---|
| Qual foi o perfil dos 2.400 disparos? | Proprietários de veículos com CPF identificável, com nome e celular válidos no RMI, e veículo principal com menos de 6 infrações não pagas |
| Multas a vencer ou multas vencidas? | **Multas vencidas e não pagas** — filtro `data_limite_recurso < CURRENT_DATE` garante que o prazo de recurso já havia expirado |
| Qual a primeira data de autuação na base? | **2023-04-30** — a query usa `data BETWEEN '2025-01-01' AND CURRENT_DATE`, então multas anteriores a 2025 ficaram de fora, mas existem na tabela desde abril de 2023 |
| A randomização levou em conta a data de vencimento da multa? | **Não.** Não houve filtro ou estratificação por data de vencimento financeiro do boleto. Todas as multas vencidas desde jan/2025 entraram igualmente no sorteio |

### Dúvidas abertas

| Pergunta | Status |
|---|---|
| Quantas multas foram pagas aproveitando o desconto de 20% e 40%? | **Não analisado ainda** — a query de elegibilidade filtra apenas não pagos; análise de pagamentos com desconto requer query separada na `autuacao` filtrando `data_pagamento IS NOT NULL` e comparando `valor_pago` com `valor_infracao` |
| A combinação de heurísticas foi testada ou apenas nudge simples? | **Não ficou claro** — a interlocutora perguntou se houve combinação de heurísticas ou uso de ameaça social; não foi respondido na reunião |
| O que é "antecipação de receitas" no contexto mencionado? | **Não ficou claro** para Patrícia e Maria Luiza — registrar para esclarecer com Thiago/SMTR |

---

## Decisões tomadas

1. **Seguir com os inadimplentes** — continuar o experimento com a base de multas vencidas; não encerrar

2. **Próximo experimento: multas a vencer** — desenhar o experimento com multas ainda dentro do prazo, mas **deixar na gaveta por ora** — não executar agora

3. **Preparar relatório completo** — com todos os cruzamentos, análises feitas e conclusões, para envio à SMTR

4. **Trazer o conteúdo da HSM** na próxima apresentação — incluir o texto exato da mensagem enviada

5. **Unir forças com o dashboard da SMTR** — Thiago informou que a SMTR já está construindo um dashboard de pagamento de multas; alinhar para integrar os dados do experimento

---

## Próximos passos

| Ação | Responsável | Prazo |
|---|---|---|
| Analisar comportamento de pagamento: quanto tempo após o disparo a pessoa paga | A definir | — |
| Medir quanto o disparo acelerou o comportamento natural de pagamento (vs. controle) | A definir | — |
| Trazer o valor total de arrecadação gerado pelo experimento nas próximas apresentações | A definir | — |
| Calcular pagamentos com desconto de 20% e 40% — query separada na `autuacao` | A definir | — |
| Preparar relatório completo com cruzamentos, análises e conclusões para SMTR | Patrícia / equipe | — |
| Desenhar experimento com multas a vencer (não executar agora) | A definir | — |
| Trazer recorte completo de tudo que foi analisado na próxima apresentação | A definir | — |
| Alinhar com Thiago sobre dashboard de pagamento de multas da SMTR | @Rodolpho Souza / @Maria Luiza Wuillaume | — |
| Incluir conteúdo da HSM na próxima apresentação | A definir | — |
| Esclarecer o que é "antecipação de receitas" com SMTR | A definir | — |

---

## Observações técnicas registradas

- **Metodologia:** nudge — sem ameaça social ou combinação de heurísticas confirmada (ponto em aberto)
- **Aversão a perdas** foi mencionada como heurística relevante para o próximo experimento, junto com desconto por pagamento antecipado
- **Corte de data na query atual:** `data BETWEEN '2025-01-01' AND CURRENT_DATE` — multas de 2023 e 2024 existem na tabela mas foram excluídas; avaliar se faz sentido ampliar o recorte no próximo experimento
- **Veículos com 6+ infrações** foram excluídos do universo — reincidentes pesados não foram alcançados; considerar se esse público merece abordagem separada
