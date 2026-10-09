# Template - Pull Request

> **Atenção:** Este template é um guia para o PR — **não o copie como mensagem de commit**. Cada commit deve ter sua própria mensagem descritiva.

---

## Checklist obrigatório

- [ ] O título do PR segue o padrão `[WIP][Tipo] descrição curta`
- [ ] O PR foi aberto como **Draft**
- [ ] Cada commit tem sua própria mensagem descritiva seguindo Conventional Commits
- [ ] O código foi testado localmente antes de solicitar revisão

---

## Título do PR

O título deve seguir o formato: `[WIP][Tipo] escopo: descrição curta e objetiva`

| Tipo | Uso | Exemplo de título |
|------|-----|-------------------|
| `[Feature]` | Nova funcionalidade | `[WIP][Feature] rj_iplanrio__taxirio: add column tipo_veiculo` |
| `[Data]` | Subida de dados em produção | `[WIP][Data] rj_smas__cadunico: upload competencia 2024-01` |
| `[Bugfix]` | Correção de bug | `[WIP][Bugfix] rj_cor__precipitacao_alertario: fix null values in taxa_precipitacao` |
| `[Refactor]` | Refatoração sem mudança de comportamento | `[WIP][Refactor] rj_smfp__nota_carioca_oracle_to_bq: simplify extraction flow` |
| `[Docs]` | Atualização de documentação | `[WIP][Docs] rj_cgm__osinfo_rh: add tables descriptions` |
| `[Test]` | Mudanças em testes | `[WIP][Test] rj_iplanrio__sicop: add unit tests for transform step` |
| `[Chore]` | Manutenção e tarefas menores | `[WIP][Chore] bump prefect to 3.4.9` |
| `[Deactivate]` | Desativação de schedule de pipeline | `[WIP][Deactivate] rj_cvl__osinfo: disable schedule` |

> Remova o prefixo `[WIP]` apenas ao marcar o PR como **Ready for Review**.

---

## Boas práticas de commit

Cada commit deve ser atômico — uma mudança lógica por commit — com mensagem própria e descritiva. Use o formato:

```
<tipo>(escopo): descrição curta no imperativo

corpo opcional explicando o "porquê" da mudança
```

**Tipos válidos:** `feat`, `fix`, `refactor`, `docs`, `test`, `chore`, `data`, `perf`

| Tipo | Quando usar | Exemplo de mensagem |
|------|-------------|---------------------|
| `feat` | Nova funcionalidade | `feat(rj_iplanrio__taxirio): add column tipo_veiculo` |
| `fix` | Correção de bug | `fix(rj_smfp__nota_carioca_oracle_to_bq): troca tudo-ou-nada e checksums` |
| `perf` | Melhoria de desempenho | `perf(rj_smfp__nota_carioca_oracle_to_bq): contagem do Oracle em paralelo` |
| `refactor` | Refatoração | `refactor(rj_cor__precipitacao_alertario): simplify extraction logic` |
| `docs` | Documentação | `docs(rj_cgm__osinfo_rh): add tables descriptions` |
| `test` | Testes | `test(rj_iplanrio__sicop): add unit test for normalize_date` |
| `chore` | Manutenção | `chore(deps): bump prefect to 3.4.9` |
| `data` | Dados em produção | `data(rj_smas__cadunico): upload competencia 2024-01` |

**Evite:**
- `fix bug`, `update`, `changes` — mensagens vagas não descrevem o que foi feito
- Commits gigantes com múltiplas responsabilidades — quebre em commits menores

---

## Descrição

**O que muda?**
_Descreva objetivamente o que foi alterado neste PR._

**Por quê muda?**
_Explique a motivação e o problema que esta mudança resolve._

---

## Detalhes técnicos

**Alterações na pipeline/scripts:**
_Descreva as principais mudanças de código ou fluxo._

**Mudanças no schema/dados:**
_Indique alterações em schema, colunas, tipos ou partições._

**Impacto em desempenho:**
_Mencione impactos relevantes ou escreva "Nenhum"._

> Se algum trecho de código precisar de atenção especial, comente diretamente na linha para os revisores.

---

## Testes e validações

- [ ] Testado localmente
- [ ] Testado na Cloud

**Observações:**
_Descreva os cenários testados, resultados ou limitações encontradas._

---

## Riscos e mitigações

**Riscos conhecidos:**
_Descreva os problemas que podem surgir com esta mudança._

**Plano de rollback:**
_Explique como reverter as mudanças caso necessário._

---

## Dependências

_Liste bibliotecas, outros PRs ou mudanças necessárias antes do merge. Se não houver, escreva "Nenhuma"._
