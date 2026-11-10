# Checklist — O que trazer em toda apresentação de teste A/B

Aprendizados da reunião de 29/10/2026 (resultados do teste A/B de multas SMTR).
Use como checklist antes de qualquer apresentação de experimento.

---

## Sobre a base

- [ ] **Perfil dos disparados** — quem são as pessoas (faixa etária, sexo, bairro, qualidade do telefone, estratégia de envio)
- [ ] **Tipo de público** — multas a vencer ou vencidas? dívida ativa ou não? inadimplente ou não? deixar explícito
- [ ] **Data de corte da base** — qual o período de autuação/geração usado e por quê
- [ ] **Filtros aplicados** — o que foi excluído do universo (ex: reincidentes, sem celular, sem nome) e o impacto de cada corte
- [ ] **A data de vencimento foi aleatorizada?** — ou todos tinham o mesmo perfil de vencimento

---

## Sobre o experimento

- [ ] **Conteúdo da HSM** — trazer o texto exato da mensagem enviada
- [ ] **Metodologia** — qual heurística foi usada (nudge, aversão a perdas, ameaça social, etc.) e se houve combinação
- [ ] **Tamanho dos grupos** — tratamento e controle, com seed de randomização registrada

---

## Sobre os resultados

- [ ] **Comportamento de pagamento pós-disparo** — distribuição do tempo entre o disparo e o pagamento
- [ ] **Aceleração vs. comportamento natural** — quanto o grupo tratamento pagou a mais/antes em relação ao controle
- [ ] **Valor total de arrecadação** — quanto entrou em reais, separado por grupo
- [ ] **Pagamentos com desconto** — quantos aproveitaram desconto (ex: 20% e 40%) e qual o impacto na arrecadação líquida
- [ ] **Recorte completo das análises feitas** — não só o resultado principal; trazer todos os cruzamentos explorados

---

## Formato

- [ ] Trazer o relatório completo com cruzamentos, análises e conclusões — não só os highlights
