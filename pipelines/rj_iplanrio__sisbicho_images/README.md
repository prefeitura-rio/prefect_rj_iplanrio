# Pipeline SISBICHO imagens (rj_iplanrio__sisbicho_images)

Lê o QR code e envia a foto de cada animal novo do SISBICHO para o bucket `portal-do-cidadao`. Depois grava uma linha por animal no BigQuery.

## Estrutura da Pipeline

- **`flow.py`**: Lista os animais novos, processa em lotes e carrega as linhas no fim.
- **`tasks.py`**: Lista os ids pendentes e processa um lote.
- **`helpers.py`**: Lê o Base64, o QR code, envia a foto e monta as linhas.
- **`constants.py`**: Tamanho do lote, tipos de arquivo e colunas de id.
- **`queries/`**: Colunas da origem, ids pendentes e dados dos animais.

## Dados

| Tabela | Origem | Descrição |
| :--- | :--- | :--- |
| `brutos_sisbicho.animal_imagem` | `brutos_sisbicho.animal` | `id_animal`, `cpf`, `qrcode_payload`, `foto_url` e `ingestao_data`. |

Um animal é novo quando tem QR code ou foto e o seu id ainda não está na tabela. A tabela é Delta, no modo `append`. Cada execução entra com o horário dela, e uma nova tentativa da mesma execução não repete linhas.

## Configurações e Deploy

- **Staging**: grava as fotos em `animal/fotos_staging`.
- **Production**: grava as fotos em `animal/fotos`. Agendamento diário às 06:00 (`cron: "0 6 * * *"`).

O parâmetro `limits` aceita `batch_size` (1000 por padrão) e `max_records` para uma execução manual menor.
