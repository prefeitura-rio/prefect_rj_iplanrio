# Prompts — movidos para o Infisical

Os arquivos de prompt (classificação/extração, todas as versões) e seus
changelogs **não são mais commitados neste repositório**. Eles vivem no
Infisical, um secret por arquivo, injetado como env var em runtime — igual
`RJ_NF_AGENT_CREDENTIALS`/`BIFROST_API_KEY` já são hoje.

## Convenção de nome

`PROMPT_{TIPO}_{VERSAO}`, maiúsculo:

- `PROMPT_CLASSIFICATION_V1` ... `PROMPT_CLASSIFICATION_V8`
- `PROMPT_EXTRACTION_V1` ... `PROMPT_EXTRACTION_V9`
- `PROMPT_CLASSIFICATION_CHANGELOG`, `PROMPT_EXTRACTION_CHANGELOG`
  (histórico de mudanças de cada prompt — não lido pelo código, é só
  documentação; guardado no Infisical pelo mesmo motivo dos prompts: o
  conteúdo do changelog descreve em detalhe o desenho do prompt).

`utils/prompts.py::list_available_versions` descobre as versões disponíveis
escaneando `os.environ` por esse prefixo — subir uma versão nova é só
adicionar o secret com o nome certo, sem mudar código.

## Onde subir (teste/prod)

Duas pastas/ambientes no Infisical, espelhando os dois k8s secrets já
usados pelo deployment (`prefect-jobs-secrets-staging` / `prefect-jobs-secrets`,
ver `prefect.yaml`):

1. Criar (se ainda não existir) um ambiente/pasta de teste e um de prod no
   projeto Infisical deste pipeline.
2. Para cada arquivo de prompt e changelog, criar um secret com o nome da
   convenção acima e colar o conteúdo do `.txt` como valor.
3. Confirmar que o sync desses secrets pro k8s (`prefect-jobs-secrets*`) já
   está configurado — é o mesmo mecanismo externo que hoje sincroniza
   `RJ_NF_AGENT_CREDENTIALS`/`BIFROST_API_KEY`/`GCS_BUCKET`, não algo que
   este repositório faz sozinho.

Os `.txt` originais (última versão antes da migração) devem ter sido salvos
localmente por quem fez esse upload — este repositório não guarda mais
cópia deles.
