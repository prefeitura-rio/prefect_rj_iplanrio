# AGENTS.md

Guia para agentes (e pessoas) que alteram este repositório. A estrutura e as convenções de código estão no
[README.md](README.md) e no [STYLEGUIDE.md](STYLEGUIDE.md): leia os dois antes de mudar uma pipeline.

## Atenção: o merge em `master` implanta produção

- Não há branch de staging à frente da `master`. O push em `master` que muda `pipelines/**` roda o workflow
  `Deploy Prefect flows (prod)` (`.github/workflows/deploy-prefect-flows-prod.yaml`), que registra no prefect-server o
  deployment `--prod` das pipelines do diff.
- Mudança no `Dockerfile` raiz roda o `Build and push root image`, que republica a imagem base e redeploya **todas** as
  pipelines nos dois ambientes. Só mexa nele quando a tarefa exigir.
- O deployment `--staging` só é registrado pelo push numa branch `staging/*` com PR aberto para `master`.
- Nunca faça push direto na `master`: toda mudança entra por PR.

## Comandos

O workspace é do [uv](https://docs.astral.sh/uv/) (Python 3.13, que o uv baixa sozinho). Rode tudo a partir da raiz do
repositório. O nome do pacote de uma pipeline é o `name` do `pyproject.toml` dela (em geral igual ao diretório).

| O quê | Comando |
| --- | --- |
| Testes de uma pipeline | `uv run --package <pipeline> --with pytest python -m pytest pipelines/<pipeline>/tests` |
| Ruff de uma pipeline (relatório completo) | `uvx ruff@0.12.5 check pipelines/<pipeline>` |
| Formatação de um arquivo | `uvx ruff@0.12.5 format <arquivo>` |
| Ruff só nas linhas alteradas (o que o CI cobra) | `uv run .github/scripts/pr_checks.py lint --base origin/master` |
| Testes das pipelines alteradas (o que o CI roda) | `uv run .github/scripts/pr_checks.py tests --base origin/master` |
| Testes do script de CI | `uv run --no-project --python 3.13 --with pytest --with ruff==0.12.5 pytest .github/scripts/tests/test_pr_checks.py` |

- Use `python -m pytest`, não `pytest` direto: os testes importam `pipelines.<pipeline>...`, e o `python -m` põe a raiz
  do repositório no `sys.path`.
- O ruff usa a configuração do `pyproject.toml` da raiz. A `master` ainda tem achados antigos, então o CI só reprova os
  achados nas linhas que o PR adiciona ou muda (e erro de sintaxe em arquivo alterado). Não corrija lint de código que a
  tarefa não toca.
- Testes ficam em `pipelines/<pipeline>/tests/` (STYLEGUIDE §3.3) e não podem depender de rede, BigQuery, bancos
  on-prem ou APIs externas: prove a lógica com teste unitário e dados sintéticos.
- O `.gitignore` ignora `**/**/test*`: arquivo de teste novo precisa de `git add -f`.
- `uv run` pode atualizar o `uv.lock` localmente; não commite essa mudança se a tarefa não for sobre dependências.

## CI do PR para `master`

- `PR checks` (`.github/workflows/pr-checks.yaml`): `Ruff (changed lines)` e `Tests (changed pipelines)`. Os testes
  rodam em cada pipeline alterada que tem `tests/` e em todas elas quando o PR muda `src/`, o `pyproject.toml` da raiz
  ou o `uv.lock`.
- `SAST` (`.github/workflows/sast.yml`), Mergify e CodeRabbit.

## Segredos

- Nunca commite segredo, `.env`, token ou dado de cidadão. Variáveis sensíveis chegam ao container pelo Secret do
  Kubernetes indicado em `job_variables.secretName` do `prefect.yaml`.
- `secretName` só pode citar um Secret que já existe no namespace `prefect-server` do cluster `iplanrio-infra`
  (declarados como `InfisicalSecret` em `iplanrio-dia/modules/k8s/prefect.tf` do `prefeitura-rio/infra`):

| Secret | Uso |
| --- | --- |
| `prefect-jobs-secrets` | deployments `--prod` |
| `prefect-jobs-secrets-staging` | deployments `--staging` |
| `prefect-jobs-crm-registry-secrets` | pipelines do CRM (`rj_crm__*` e as de disparo de `rj_smas`), `--prod` |
| `prefect-jobs-crm-registry-secrets-staging` | as mesmas, `--staging` |
| `prefect-jobs-ssm-secrets` | pipelines `rj_ssm__*` |

- Secret novo, ou variável nova num deles, muda no `prefeitura-rio/infra` ou no Infisical, nunca neste repositório.

## Pull requests

- Commits em Conventional Commits (`feat`, `fix`, `refactor`, `docs`, `test`, `chore`, `data`, `perf`), uma mudança
  lógica por commit.
- PRs abertos por automação (como a esteira da IPLAN) não são draft e usam o título `<tipo>(<escopo>): <resumo> [KEY]`;
  o prefixo `[WIP]` e o Draft do template valem para os PRs abertos por pessoas.
