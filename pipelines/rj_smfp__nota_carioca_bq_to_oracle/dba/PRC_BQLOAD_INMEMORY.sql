-- DFEN.PRC_BQLOAD_INMEMORY: liga ou desliga o In-Memory das tabelas A/B da carga BigQuery -> Oracle da
-- Nota Carioca (pipeline rj_smfp__nota_carioca_bq_to_oracle).
--
-- Por que existe: o usuario da carga cria, carrega e troca as tabelas DFEN.BQLOAD_*_A/_B, mas nao tem
-- ALTER ANY TABLE. A procedure roda com os direitos do dono (AUTHID DEFINER) e aceita SO tabelas
-- BQLOAD_*_A/_B do DFEN que tenham no comentario a marca da pipeline. A clausula de In-Memory e fixa.
--
-- Uso pela pipeline:
--   - antes de carregar a tabela que esta fora de uso: p_ativar => 0 (tabela inteira, NO INMEMORY)
--   - depois da carga: p_ativar => 1 em cada particao da janela (mes corrente e os 3 anteriores)
--   - alguns minutos depois da troca: p_ativar => 0 na tabela que saiu de uso, liberando a memoria
--
-- Nao mata sessao: com o objeto ocupado, o ALTER falha com ORA-00054 depois do DDL_LOCK_TIMEOUT
-- curto e a pipeline tenta de novo (mesmo padrao do DFEN.PRC_ROLL_IMCS_NACIONAL).
--
-- Executar como DFEN (ou DBA com CREATE ANY PROCEDURE), em homologacao (HNFSE) e em producao (NFSE).

CREATE OR REPLACE PROCEDURE DFEN.PRC_BQLOAD_INMEMORY (
    p_tabela   IN VARCHAR2,
    p_particao IN VARCHAR2 DEFAULT NULL,
    p_ativar   IN NUMBER
) AUTHID DEFINER AS
    c_marca      CONSTANT VARCHAR2(64)  := 'rj_smfp__nota_carioca_bq_to_oracle';
    c_inmemory   CONSTANT VARCHAR2(200) := 'INMEMORY PRIORITY HIGH MEMCOMPRESS FOR QUERY HIGH DISTRIBUTE AUTO NO DUPLICATE';
    v_tabela     VARCHAR2(128) := UPPER(p_tabela);
    v_particao   VARCHAR2(128) := UPPER(p_particao);
    v_comentario VARCHAR2(4000);
    v_existe     PLS_INTEGER;
    v_clausula   VARCHAR2(200);
BEGIN
    IF p_ativar = 1 THEN
        v_clausula := c_inmemory;
    ELSIF p_ativar = 0 THEN
        v_clausula := 'NO INMEMORY';
    ELSE
        RAISE_APPLICATION_ERROR(-20901, 'PRC_BQLOAD_INMEMORY: p_ativar deve ser 0 ou 1');
    END IF;

    IF v_tabela IS NULL OR NOT REGEXP_LIKE(v_tabela, '^BQLOAD_[A-Z0-9_$#]+_[AB]$') THEN
        RAISE_APPLICATION_ERROR(-20902, 'PRC_BQLOAD_INMEMORY: tabela fora do escopo: ' || p_tabela);
    END IF;

    BEGIN
        SELECT comments INTO v_comentario
          FROM user_tab_comments
         WHERE table_name = v_tabela
           AND table_type = 'TABLE';
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            RAISE_APPLICATION_ERROR(-20903, 'PRC_BQLOAD_INMEMORY: tabela inexistente: ' || p_tabela);
    END;
    IF v_comentario IS NULL OR SUBSTR(v_comentario, 1, LENGTH(c_marca)) <> c_marca THEN
        RAISE_APPLICATION_ERROR(-20904, 'PRC_BQLOAD_INMEMORY: tabela nao foi criada pela pipeline: ' || p_tabela);
    END IF;

    EXECUTE IMMEDIATE 'ALTER SESSION SET DDL_LOCK_TIMEOUT = 8';

    IF v_particao IS NULL THEN
        EXECUTE IMMEDIATE 'ALTER TABLE "' || v_tabela || '" ' || v_clausula;
    ELSE
        SELECT COUNT(*) INTO v_existe
          FROM user_tab_partitions
         WHERE table_name = v_tabela
           AND partition_name = v_particao;
        IF v_existe = 0 THEN
            RAISE_APPLICATION_ERROR(-20905, 'PRC_BQLOAD_INMEMORY: particao inexistente: ' || p_tabela || '.' || p_particao);
        END IF;
        EXECUTE IMMEDIATE 'ALTER TABLE "' || v_tabela || '" MODIFY PARTITION "' || v_particao || '" ' || v_clausula;
    END IF;
END PRC_BQLOAD_INMEMORY;
/

-- Usuario da carga (o mesmo em homologacao e em producao).
GRANT EXECUTE ON DFEN.PRC_BQLOAD_INMEMORY TO "26234793";
