# Guia Completo para LLMs - Projeto Prefect RJ IplanRio

Este documento consolida informações essenciais para LLMs trabalharem com o projeto Prefect RJ IplanRio, incluindo workflows de desenvolvimento e migração de Prefect 1.4 para 3.0.

## 📋 Visão Geral do Projeto

O projeto Prefect RJ IplanRio é um sistema de pipelines de dados para a Prefeitura do Rio de Janeiro, utilizando Prefect 3.0 para orquestração de workflows. O projeto segue convenções específicas de nomenclatura e estrutura.

### Convenções de Nomenclatura
- **Pipelines**: `rj_<secretaria>__<pipeline>`
- **Exemplos**: `rj_crm__api_wetalkie`, `rj_smas__cadunico`, `rj_iplanrio__dados_mestres`

## 🛠️ Workflow de Desenvolvimento

### Comandos Essenciais

#### Desenvolvimento
```sh
# Instalar dependências
uv sync

# Criar nova pipeline
uvx cookiecutter templates --output-dir=pipelines

# Executar pipeline localmente
prefect flow run <flow-name>
```

#### Deploy
```sh
# Deploy manual (se necessário)
prefect deployment build <flow-file>:<flow-name> -n <deployment-name>
prefect deployment apply <deployment-name>
```

### Criação de Novas Pipelines

1. **Use cookiecutter**: `uvx cookiecutter templates --output-dir=pipelines`
2. **Siga convenções**: Nomenclatura `rj_<secretaria>__<pipeline>`
3. **Configure prefect.yaml**: Work pool e secrets apropriados
4. **Teste localmente**: Antes do deploy
5. **Use staging**: Antes de produção

### Estrutura de Arquivos

- **Arquivos principais**: Mantenha no diretório raiz da pipeline
- **Subdiretórios**: Use `utils/` apenas quando necessário
- **Templates**: Siga a estrutura de templates para novas pipelines
- **Simplicidade**: Não crie arquivos desnecessários

## 🔧 Troubleshooting

### Problemas Comuns

- **Erro de nomenclatura**: Verifique se segue padrão `rj_<secretaria>__<pipeline>`
- **Secret não encontrado**: Confirme se `secretName` está correto no `prefect.yaml`
- **Pool incorreto**: Verifique se está usando `datario-pool` apenas quando necessário
- **Dependências**: Use `uv sync` para sincronizar workspace

### Logs e Debug

- **GitHub Actions**: Verifique logs para problemas de deploy
- **Prefect UI**: Use para monitorar execução de flows
- **Documentação**: Consulte documentação oficial do Prefect para detalhes específicos

## 🔄 Migração Prefect 1.4 para 3.0

### Processo de Migração

Quando solicitado para migrar um flow do Prefect 1.4 para 3.0:

#### 1. Migrar o Schedule
- Analise o `schedule` do Prefect 1.4 (arquivo original fornecido)
- Converta para formato compatível com Prefect 3.0
- **SEMPRE use o template** `templates/rj_{{ cookiecutter.secretaria }}__{{ cookiecutter.pipeline }}/prefect.yaml` como modelo
- Gere novo arquivo YAML seguindo a estrutura do template

#### 2. Adaptar o Flow
- **SEMPRE use o template** `templates/rj_{{ cookiecutter.secretaria }}__{{ cookiecutter.pipeline }}/flow.py` como modelo
- Modifique o flow de destino mantendo funcionalidade equivalente
- Adapte nomes de variáveis, funções e fluxo conforme o nome da pasta
- Garanta compatibilidade total com Prefect 3.0
- Mantenha a estrutura e padrões do template

#### 3. Output Esperado
- Conteúdo final do YAML convertido baseado no template
- Novo flow modificado seguindo o padrão do template
- Comentários resumindo alterações em relação aos originais

### Arquivos de Referência Obrigatórios

**SEMPRE use estes templates como referência:**
- `templates/rj_{{ cookiecutter.secretaria }}__{{ cookiecutter.pipeline }}/flow.py` - Template de flow Prefect 3.0
- `templates/rj_{{ cookiecutter.secretaria }}__{{ cookiecutter.pipeline }}/prefect.yaml` - Template de configuração

### Principais Mudanças Prefect 3.0

- Nova sintaxe de flows e tasks
- Mudanças na configuração de deployments
- Atualizações na estrutura de schedules
- Novos padrões de logging e monitoramento
- Alterações na API e métodos de execução

## 📁 Estrutura de Templates

### Flow Template (flow.py)
```python
@flow(log_prints=True)
def rj_{{ cookiecutter.secretaria }}__{{ cookiecutter.pipeline }}(
    # Parâmetros específicos da pipeline
):
    # Implementação do flow
```

### Prefect.yaml Template
```yaml
name: rj_{{ cookiecutter.secretaria }}__{{ cookiecutter.pipeline }}
prefect-version: 3.4.3

build:
  - prefect.deployments.steps.run_shell_script:
      id: get-commit-hash
      script: git rev-parse --short HEAD
  - prefect_docker.deployments.steps.build_docker_image:
      id: build-image
      image_name: ghcr.io/prefeitura-rio/prefect_rj_iplanrio/deployments
      tag: "rj_{{ cookiecutter.secretaria }}__{{ cookiecutter.pipeline }}-{% raw %}{{ get-commit-hash.stdout }}{% endraw %}"

deployments:
  - name: rj-{{ cookiecutter.secretaria | lower }}--{{ cookiecutter.pipeline }}--staging
    work_pool:
      name: k3s-pool
      job_variables:
        secretName: prefect-jobs-secrets-staging
  - name: rj-{{ cookiecutter.secretaria | lower }}--{{ cookiecutter.pipeline }}--prod
    schedules:
      - cron: "0 0 * * *"
    work_pool:
      name: k3s-pool
      job_variables:
        secretName: prefect-jobs-secrets
```

## 🎯 Diretrizes para LLMs

### Ao Trabalhar com o Projeto

1. **Sempre use os templates** como referência para novos flows e configurações
2. **Siga as convenções de nomenclatura** rigorosamente
3. **Mantenha a estrutura simples** - evite arquivos desnecessários
4. **Teste localmente** antes de sugerir deploys
5. **Use staging** como ambiente intermediário

### Ao Migrar Flows

1. **Analise o flow original** completamente
2. **Use os templates obrigatórios** como base
3. **Mantenha a funcionalidade equivalente**
4. **Adapte nomes** conforme a convenção da pasta
5. **Documente as mudanças** realizadas

### Ao Criar Novas Pipelines

1. **Use cookiecutter** para gerar a estrutura base
2. **Configure prefect.yaml** com work pools e secrets corretos
3. **Implemente o flow** seguindo o template
4. **Teste localmente** antes do deploy
5. **Configure schedules** apropriados

---

**Nota**: Este guia deve ser consultado sempre que trabalhar com o projeto Prefect RJ IplanRio para garantir consistência e seguir as melhores práticas estabelecidas.