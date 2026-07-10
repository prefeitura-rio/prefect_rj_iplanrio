# Pipelines - Prefect

Este repositório contém pipelines desenvolvidas com Prefect para automação de fluxos de dados da Prefeitura do Rio de Janeiro.

## Contribuindo com o projeto

Se você deseja contribuir com o projeto, acesse nossa [guia de contribuição](https://iplan-rio.mintlify.app/data-lake/prefect/construindo-uma-pipeline).

## Estrutura do projeto

### Raiz do repositório

```
pipelines/
├── rj_sec__pipeline1/
├── rj_sec__pipeline2/
└── ...
src/
└── prefect_rj_iplanrio/
    └── __init__.py
Dockerfile
pyproject.toml
uv.lock
```

### Pipeline

```
rj_sec__pipeline
├── Dockerfile
├── flow.py
├── prefect.yaml
├── pyproject.toml
└── ...
```

## Como usar este repositório

Este repositório utiliza um modelo monorepo para gerenciar múltiplas pipelines Prefect de forma centralizada e padronizada:

- **Dockerfile base**: o arquivo `Dockerfile` na raiz define a imagem base utilizada por todas as pipelines. Ele inclui as dependências essenciais para execução dos fluxos, como Python, Prefect, drivers de banco de dados e ferramentas auxiliares. Cada pipeline pode customizar sua própria imagem a partir desse Dockerfile base, garantindo consistência e facilidade de manutenção.

- **`pyproject.toml` centralizado com uv workspaces**: o `pyproject.toml` na raiz do projeto gerencia as dependências Python de todas as pipelines utilizando [uv workspaces](https://docs.astral.sh/uv/concepts/projects/workspaces/).
  - Cada subdiretório em `pipelines/` representa um módulo Python independente, mas todos são importados automaticamente como membros do workspace.
  - As pipelines declaram `prefect_rj_iplanrio` como dependência com `{ workspace = true }`, herdando automaticamente as dependências base (`iplanrio`, `prefect`, `pandas`, etc.).
  - Novas pipelines adicionadas à pasta `pipelines/` são automaticamente reconhecidas e integradas ao workspace, sem necessidade de configuração manual adicional.

- **Código compartilhado**: o diretório `src/prefect_rj_iplanrio/` contém código Python compartilhado entre as pipelines.
  - Funções utilitárias, classes base, helpers e lógica comum podem ser adicionados neste pacote.
  - Todas as pipelines que dependem de `prefect_rj_iplanrio` podem importar este código diretamente:
    ```python
    from prefect_rj_iplanrio import minha_funcao_compartilhada
    ```
  - Isso evita duplicação de código e facilita a manutenção de funcionalidades reutilizáveis.

Esse modelo garante reprodutibilidade, isolamento e facilidade de evolução para todas as pipelines do repositório.

### Pipelines

Cada pipeline possui:

- `Dockerfile`: imagem customizada para execução no Prefect
- `flow.py`: definição dos fluxos Prefect
- `prefect.yaml`: configuração do deployment Prefect
- `pyproject.toml`: dependências específicas da pipeline

Alterações específicas de cada pipeline devem ser adicionadas no `Dockerfile` de cada pipeline, assim como dependências específicas devem ser adicionadas no `pyproject.toml` de cada pipeline.

O arquivo `prefect.yaml` é responsável por definir as configurações de deployment do flow Prefect. Esse arquivo especifica variáveis de ambiente, parâmetros, agendamento, infraestrutura de execução e outras opções necessárias para o correto funcionamento do fluxo. O arquivo também é utilizado nos workflows de CI/CD para registrar e disponibilizar os flows.

Consulte a [documentação oficial do Prefect](https://docs.prefect.io/v3/how-to-guides/deployments/prefect-yaml) para detalhes sobre todas as opções disponíveis.

#### Configuração de secrets

O parâmetro `secretName` nas `job_variables` do `prefect.yaml` especifica qual Kubernetes Secret deve ser montado no container durante a execução do job. Esse secret contém variáveis de ambiente necessárias para o funcionamento da pipeline, como credenciais de banco de dados, tokens de API e outras informações sensíveis.

**Valores padrão utilizados:**

- **`prefect-jobs-secrets`**: para deployments de produção
- **`prefect-jobs-secrets-staging`**: para deployments de staging

Exemplo de configuração no `prefect.yaml`:

```yaml
deployments:
  - name: rj-secretaria--pipeline--prod
    work_pool:
      job_variables:
        secretName: prefect-jobs-secrets
  - name: rj-secretaria--pipeline--staging
    work_pool:
      job_variables:
        secretName: prefect-jobs-secrets-staging
```

### Convenções de nomenclatura

- Os diretórios de pipelines seguem o padrão: `rj_<secretaria>__<pipeline>`, utilizando apenas letras minúsculas, números e underscores.
- Os arquivos principais de cada pipeline (`Dockerfile`, `flow.py`, `prefect.yaml`, `pyproject.toml`) devem estar diretamente dentro do diretório da pipeline.
- Os nomes de deployments Prefect seguem o padrão `rj-<secretaria>--<pipeline>--<ambiente>`, por exemplo: `rj-segovi--dump-db-1746--staging`.
- Use sempre nomes descritivos e padronizados para facilitar a identificação e automação dos fluxos.

### Templates

Este repositório utiliza [`cookiecutter`](https://cookiecutter.readthedocs.io/) para facilitar a criação de novas pipelines de forma padronizada. Os templates disponíveis em `templates/` permitem gerar rapidamente a estrutura de diretórios e arquivos necessários para uma nova pipeline Prefect, incluindo `Dockerfile`, `flow.py`, `prefect.yaml` e `pyproject.toml`.

Para criar uma nova pipeline, instale `uv` e rode:

```sh
uvx cookiecutter templates --output-dir=pipelines
```

Você será solicitado a informar valores como `secretaria` e `pipeline`, que serão utilizados para preencher os nomes dos diretórios, arquivos e variáveis nos templates. O template gerado já segue o padrão de nomenclatura definido anteriormente.

### Work pools

Este repositório utiliza dois work pools principais para execução dos deployments Prefect:

- **default-pool**: destinado à execução geral de pipelines, incluindo fluxos que não possuem requisitos especiais de rede ou infraestrutura. É o pool padrão para a maioria dos deployments.
- **k3s-pool**: utilizado para pipelines que acessam bancos de dados ou sistemas internos da IplanRio, especialmente aqueles que exigem conexão via VPN. Esse pool garante que os jobs sejam executados em ambientes com acesso seguro e autorizado aos recursos internos.

Ao configurar um deployment no arquivo `prefect.yaml`, selecione o pool apropriado conforme a necessidade de acesso aos dados ou sistemas.

## CI/CD

O repositório utiliza GitHub Actions para automatizar todo o ciclo de vida das pipelines, incluindo build, deploy e publicação de imagens Docker:

- **Deploy automático dos flows**: os workflows `.github/workflows/deploy-prefect-flows-prod.yaml` e `.github/workflows/deploy-prefect-flows-staging.yaml` realizam o deploy automático dos flows Prefect para os ambientes de produção e staging, respectivamente.
  - O workflow de produção é acionado a cada push na branch `master` ou manualmente, sempre que houver alterações em arquivos dentro de `pipelines/**`.
  - O workflow de staging é acionado a cada push em branches `staging/*` ou manualmente, também monitorando alterações em `pipelines/**`.
  - Ambos executam:
    - Checkout do código-fonte
    - Login no GitHub Container Registry (`ghcr.io`)
    - Instalação das dependências Python com `uv`
    - Execução do script `.github/scripts/deploy_prefect_flows.py`, que faz o deploy automático de todos os flows definidos em `pipelines/*/prefect.yaml`
    - Caso algum deploy falhe, o workflow é interrompido e o erro é registrado nos logs

- **Build e publicação da imagem base**: o workflow `.github/workflows/build-and-push-root-dockerfile.yaml` é acionado em alterações no `Dockerfile` da raiz a cada push na branch `master`, além de poder ser executado manualmente. Ele realiza:
  - Build da imagem Docker definida no `Dockerfile` do repositório
  - Publicação da imagem no GitHub Container Registry (`ghcr.io/${{ github.repository }}:latest`)

## Troubleshooting

### Erro: `uv sync --package` falha com "package not found"

**Causa**: O `pyproject.toml` da pipeline não declara `prefect_rj_iplanrio` como dependência com `workspace = true`.

**Solução**: Adicione ao `pyproject.toml` da pipeline:

```toml
dependencies = [
    "prefect_rj_iplanrio",
    # ... outras dependências
]

[tool.uv.sources]
prefect_rj_iplanrio = { workspace = true }
```

### Erro: Docker build falha com "cx-oracle" ou "pkg_resources"

**Causa**: O pacote `cx-oracle` não é compatível com Python 3.13+.

**Solução**: O `pyproject.toml` raiz já contém um override para usar `oracledb` no lugar de `cx-oracle`. Certifique-se de que o `uv.lock` está atualizado:

```sh
uv lock
```

### Erro: Dependências base não instaladas no container

**Causa**: O Dockerfile não está copiando o diretório `src/`.

**Solução**: Verifique se o Dockerfile da pipeline contém:

```dockerfile
COPY ./pyproject.toml ./uv.lock /opt/prefect/prefect_rj_iplanrio/
COPY ./src ./src/
COPY ./pipelines/<nome-da-pipeline> ./pipelines/<nome-da-pipeline>/
RUN uv sync --package <nome-da-pipeline>
```

### Erro: Lockfile desatualizado após adicionar nova pipeline

**Causa**: O `uv.lock` precisa ser regenerado quando novas pipelines são adicionadas ao workspace.

**Solução**: Execute na raiz do repositório:

```sh
uv lock
```

### Erro: Import de código compartilhado não funciona

**Causa**: O pacote `prefect_rj_iplanrio` não está sendo instalado corretamente.

**Solução**: Verifique se:
1. O diretório `src/prefect_rj_iplanrio/` existe e contém `__init__.py`
2. A pipeline declara `prefect_rj_iplanrio` como dependência com `workspace = true`
3. O Dockerfile copia o diretório `src/`
