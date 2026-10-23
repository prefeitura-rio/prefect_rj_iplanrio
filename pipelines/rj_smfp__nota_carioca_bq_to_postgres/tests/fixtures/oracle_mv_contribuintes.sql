-- Cópia de testdata/ddl/oracle/04_nfse_rpe_contribuintes.sql de prefeitura-rio/nota-carioca-nuvem, commit 5e9cccf
-- (main, merge da PR #1 feat/servico-gateway): a DDL provisória de NFSE_RPE.MV_CONTRIBUINTES do R1, para os testes do
-- sincronizador não dependerem de caminho de outro repositório. O conteúdo abaixo é literal.
--
-- PROVISORIO: substituir pelo DDL real capturado (ops/dba/03)
--
-- NFSE_RPE.MV_CONTRIBUINTES: na produção é uma MV; aqui uma tabela comum (B1, D6).
-- Equivalente Oracle de public.mv_contribuintes em migrations/00005_espelho_cdc.sql: as marcas
-- PROVISORIO são as mesmas de lá (cpf_cnpj varchar(14) não é marcada). _sincronizado_em é
-- metadado do Postgres e não existe no Oracle.

CREATE TABLE NFSE_RPE.MV_CONTRIBUINTES (
    INSCRICAO              VARCHAR2(15) NOT NULL,  -- PROVISORIO
    CPF_CNPJ               VARCHAR2(14),
    INSCRICAO_NOTANACIONAL VARCHAR2(15),           -- PROVISORIO
    PEDAGIO                NUMBER,                 -- PROVISORIO
    CATEGORIA              NUMBER,                 -- PROVISORIO
    ATIVO                  NUMBER,                 -- PROVISORIO
    CONSTRAINT PK_MV_CONTRIBUINTES PRIMARY KEY (INSCRICAO)  -- PROVISORIO
);

CREATE INDEX NFSE_RPE.IX_MV_CONTRIBUINTES_CPF_CNP
    ON NFSE_RPE.MV_CONTRIBUINTES (CPF_CNPJ, INSCRICAO);  -- PROVISORIO: só o nome vem do hint de B1
