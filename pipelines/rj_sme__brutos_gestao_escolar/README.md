# Documentação - Gestão Escolar


## 1. Visão Geral
As tabelas de Gestão Escolar são administradas pela Secretaria Municipal de Educação (SME), sendo o órgão da Prefeitura do Rio de Janeiro responsável por elaborar a política educacional do município do Rio de Janeiro, coordenar a sua implantação e avaliar os resultados, com o objetivo de assegurar a excelência na Educação Infantil e no Ensino Fundamental Público.

## 2. Contatos
1. Equipe Responsável:
1. Nome Responsável:
2. Email Responsável

## 2. Fonte de Dados

| Item              | Valor                                  |
|-------------------|----------------------------------------|
| Origem            | SQL Server                             |
| Responsável       | Secretaria Municipal de Educação (SME) |
| Forma de ingestão | Extração direta do banco de dados      |

---

## 3. Tabelas

| Tabelas               | Descrição | Frequência de atualização | Horário de Atualização | Coluna de Partição | Materialização | Observação
|-----------------------|--------------------------------------------------| --------| ------| ---------------| ------------| ----------------------|
| turma_aula_aluno      | Frequência dos alunos por plano de aula.         | Diária  | 03:00 | data_alteracao | Incremental | Pipeline com D - 1
| turma                 | Turma de alunos, atuais ou passadas.             | Diária  | 03:30 | ano            | Tabela      |-



## 4. Regras de negócio

## Tabela: turma_aula_aluno
### [1.0.0] - 2026/06/23
1. A pipeline de turma_aula_aluno roda com D - 1 devido a um problema de crashed das pipelines que demoravam a serem iniciadas.
2. A coluna de data_alteracao é modificada diariamente, pois a cada insert na tabela, ela é alterada.

### [1.0.1] - 2026/06/26
2. A coluna de particionamento foi alterada de data_alteracao para data_criacao

