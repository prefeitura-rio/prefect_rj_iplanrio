-- Seções Up das migrações goose 00001 a 00010 de prefeitura-rio/nota-carioca-nuvem (pasta migrations/),
-- commit 5e9cccf (main, merge da PR #1 feat/servico-gateway). Cópia para os testes não dependerem de caminho
-- de outro repositório. As diretivas `-- +goose ...` foram removidas e o Down descartado; o resto é literal.
-- Para atualizar: copiar de novo as seções Up e trocar o commit citado acima.

-- ===== 00001_papeis.sql =====
-- Papéis de grupo (NOLOGIN) do esquema de serving.
--
--   nc_owner          reservado para a propriedade dos objetos (ainda sem uso)
--   nc_gateway_ro     gateway: somente SELECT em views, espelho e marca d'água
--   nc_carregador     carga dos slots exigiveis_a/b e chamada de publicar_exigiveis
--   nc_aplicador      aplicador CDC: escreve só no espelho, marca d'água e reconciliação
--   nc_sincronizador  sincronizador: escreve só em mv_contribuintes e marca d'água
--
-- Usuários LOGIN reais são criados fora das migrações e entram nestes grupos.
-- Papéis são globais ao cluster: o Up só cria o que não existe e carimba o
-- comentário do papel; o Down só remove papéis com esse carimbo.

DO $$
DECLARE
    v_papel text;
BEGIN
    FOREACH v_papel IN ARRAY ARRAY['nc_owner', 'nc_gateway_ro', 'nc_carregador', 'nc_aplicador', 'nc_sincronizador']
    LOOP
        IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = v_papel) THEN
            EXECUTE format('CREATE ROLE %I NOLOGIN', v_papel);
            EXECUTE format('COMMENT ON ROLE %I IS %L', v_papel, 'criado pela migração goose 00001_papeis');
        END IF;
    END LOOP;
END
$$;


-- ===== 00002_exigiveis_tabelas.sql =====
-- Slots A/B das notas nacionais exigíveis: schemas, tabelas e partições.
--
-- Cada slot (exigiveis_a, exigiveis_b) tem as duas tabelas das MVs do Oracle.
-- O dbt/BigQuery carrega o slot inativo e publicar_exigiveis() vira as views
-- public.* para ele (migração 00004). Nomes em minúsculas, na ordem das MVs.
--
-- Regra de mapeamento: NUMBER(p,s) -> numeric(p,s); NUMBER sem precisão ->
-- numeric; DATE -> timestamp(0) without time zone; VARCHAR2(n) -> varchar(n);
-- CHAR(n) -> char(n); RAW(16) -> bytea.
--
-- Fontes (todas de leitura, fora deste repositório):
--   MV-D = /home/zolio/work/nfs-e, origin/staging:
--          NFSE.DBL/Scripts/db/DFEN/materialized_views/MVT_NOTAS_NACIONAIS_EXIGIVEIS_DETALHES.sql
--   MV-R = idem, .../MVT_NOTAS_NACIONAIS_EXIGIVEIS.sql
--   DPS  = /home/zolio/work/ddl/DPS.sql
--   NN   = /home/zolio/work/ddl/NOTAS_NACIONAIS.sql
--   PN   = /home/zolio/work/ddl/PESSOAS_NACIONAIS.sql
--   dbt  = /home/zolio/work/queries-rj-iplanrio/models/mart/smfp/nota_carioca/
--          mart_nota_carioca__notas_nacionais_exigiveis_detalhes.sql (e a de resumo)
--
-- mvt_notas_nacionais_exigiveis_detalhes
--  #  coluna                      Oracle (origem)                 Postgres           fonte (arquivo:linha)
--  1  cpf_cnpj_responsavel        VARCHAR2(14) NOT NULL           varchar(14) NN     MV-D:13 <- PN:3
--  2  nome_contraparte            VARCHAR2(300)                   varchar(300)       MV-D:14 <- PN:8
--  3  cpf_cnpj_contraparte        VARCHAR2(14) NOT NULL           varchar(14) NN     MV-D:15 <- PN:3
--  4  retencao                    CASE tipo_retencao_issqn 0/1    numeric NN         MV-D:16 (expr. sobre DPS:43)
--  5  valor_servico               NUMBER(17,2) NOT NULL           numeric(17,2) NN   MV-D:17 <- DPS:26
--  6  valor_ded_red               NUMBER(17,2)                    numeric(17,2)      MV-D:18 <- NN:19
--  7  valor_issqn                 NUMBER(17,2)                    numeric(17,2)      MV-D:19 <- NN:24
--  8  valor_base_calculo          NUMBER(17,2)                    numeric(17,2)      MV-D:20 <- NN:22
--  9  nota_fiscal                 NUMBER(15,0) NOT NULL           numeric(15,0) NN   MV-D:21 <- NN:4
-- 10  chave_acesso                VARCHAR2(50) NOT NULL           varchar(50) NN     MV-D:22 <- NN:3
-- 11  data_validacao              DATE NOT NULL                   timestamp(0) NN    MV-D:23 <- NN:17
-- 12  data_competencia_municipio  DATE NOT NULL                   timestamp(0) NN    MV-D:24 <- NN:31 (= DPS:67)
-- 13  tipo_retencao               NUMBER(1,0)                     numeric(1,0)       MV-D:25 <- DPS:43
-- 14  nota_nacional               VARCHAR2(53) NOT NULL           varchar(53) NN     MV-D:26 <- NN:2
-- 15  dps                         VARCHAR2(45) NOT NULL           varchar(45) NN     MV-D:27 <- NN:18
-- 16  opcao_simples_nacional      NUMBER(1,0)                     numeric(1,0)       MV-D:28 <- PN:23
-- (as colunas *_ROWID da MV Oracle e os metadados _bigquery_* do dbt não são servidos)
--
-- mvt_notas_nacionais_exigiveis (agregada; GROUP BY das cinco primeiras colunas)
--  #  coluna                      Oracle (origem)                 Postgres           fonte (arquivo:linha)
--  1  cpf_cnpj_responsavel        chave de grupo                  varchar(14) NN     MV-R:18, MV-R:33
--  2  data_competencia_municipio  chave de grupo (DATE)           timestamp(0) NN    MV-R:19
--  3  tipo_retencao               chave de grupo (pode ser NULL)  numeric(1,0)       MV-R:20
--  4  opcao_simples_nacional      chave de grupo (pode ser NULL)  numeric(1,0)       MV-R:21
--  5  retencao                    chave de grupo                  numeric NN         MV-R:22
--  6  quantidade_notas            COUNT(*)                        numeric NN         MV-R:23
--  7  total_valor_servico         SUM(NUMBER(17,2))               numeric            MV-R:24
--  8  total_valor_ded_red         SUM(NUMBER(17,2))               numeric            MV-R:25
--  9  total_valor_issqn           SUM(NUMBER(17,2))               numeric            MV-R:26
-- 10  total_valor_base_calculo    SUM(NUMBER(17,2))               numeric            MV-R:27
-- 11  cnt_valor_servico           COUNT(coluna)                   numeric NN         MV-R:28
-- 12  cnt_valor_ded_red           COUNT(coluna)                   numeric NN         MV-R:29
-- 13  cnt_valor_issqn             COUNT(coluna)                   numeric NN         MV-R:30
-- 14  cnt_valor_base_calculo      COUNT(coluna)                   numeric NN         MV-R:31
--
-- Nenhuma coluna é PROVISORIO neste arquivo: todos os tipos saem do DDL/MV.
-- tipo_retencao e opcao_simples_nacional são anuláveis no Oracle (DPS:43, PN:23);
-- por isso a chave natural do resumo é um índice único NULLS NOT DISTINCT
-- (migração 00003) e não uma PRIMARY KEY, que exigiria NOT NULL.
-- A tabela de detalhes é particionada por mês de data_competencia_municipio
-- (2026-01 a 2030-12) mais uma partição DEFAULT.

DO $$
DECLARE
    v_slot     text;
    v_esquema  text;
    v_mes      date;
    v_inicio   date := DATE '2026-01-01';
BEGIN
    FOREACH v_slot IN ARRAY ARRAY['a', 'b']
    LOOP
        v_esquema := 'exigiveis_' || v_slot;
        EXECUTE format('CREATE SCHEMA %I', v_esquema);

        EXECUTE format($ddl$
            CREATE TABLE %I.mvt_notas_nacionais_exigiveis_detalhes (
                cpf_cnpj_responsavel       varchar(14)    NOT NULL,
                nome_contraparte           varchar(300),
                cpf_cnpj_contraparte       varchar(14)    NOT NULL,
                retencao                   numeric        NOT NULL,
                valor_servico              numeric(17,2)  NOT NULL,
                valor_ded_red              numeric(17,2),
                valor_issqn                numeric(17,2),
                valor_base_calculo         numeric(17,2),
                nota_fiscal                numeric(15,0)  NOT NULL,
                chave_acesso               varchar(50)    NOT NULL,
                data_validacao             timestamp(0) without time zone NOT NULL,
                data_competencia_municipio timestamp(0) without time zone NOT NULL,
                tipo_retencao              numeric(1,0),
                nota_nacional              varchar(53)    NOT NULL,
                dps                        varchar(45)    NOT NULL,
                opcao_simples_nacional     numeric(1,0)
            ) PARTITION BY RANGE (data_competencia_municipio)
            $ddl$, v_esquema);

        FOR i IN 0..59
        LOOP
            v_mes := (v_inicio + make_interval(months => i))::date;
            EXECUTE format(
                'CREATE TABLE %I.%I PARTITION OF %I.mvt_notas_nacionais_exigiveis_detalhes FOR VALUES FROM (%L) TO (%L)',
                v_esquema,
                'mvt_nn_exigiveis_detalhes_p' || to_char(v_mes, 'YYYYMM'),
                v_esquema,
                v_mes,
                (v_mes + INTERVAL '1 month')::date);
        END LOOP;

        EXECUTE format(
            'CREATE TABLE %I.mvt_nn_exigiveis_detalhes_pdefault PARTITION OF %I.mvt_notas_nacionais_exigiveis_detalhes DEFAULT',
            v_esquema, v_esquema);

        EXECUTE format($ddl$
            CREATE TABLE %I.mvt_notas_nacionais_exigiveis (
                cpf_cnpj_responsavel       varchar(14)    NOT NULL,
                data_competencia_municipio timestamp(0) without time zone NOT NULL,
                tipo_retencao              numeric(1,0),
                opcao_simples_nacional     numeric(1,0),
                retencao                   numeric        NOT NULL,
                quantidade_notas           numeric        NOT NULL,
                total_valor_servico        numeric,
                total_valor_ded_red        numeric,
                total_valor_issqn          numeric,
                total_valor_base_calculo   numeric,
                cnt_valor_servico          numeric        NOT NULL,
                cnt_valor_ded_red          numeric        NOT NULL,
                cnt_valor_issqn            numeric        NOT NULL,
                cnt_valor_base_calculo     numeric        NOT NULL
            )
            $ddl$, v_esquema);
    END LOOP;
END
$$;


-- ===== 00003_exigiveis_indices.sql =====
-- Índices dos slots A/B conforme guia-nacional/05-contrato-de-dados.md §5.3.
--
-- Resumo: o contrato pede PRIMARY KEY (data_competencia_municipio,
-- cpf_cnpj_responsavel, tipo_retencao, opcao_simples_nacional, retencao), mas
-- tipo_retencao e opcao_simples_nacional são anuláveis no Oracle. Uma PK
-- exigiria NOT NULL e rejeitaria a carga; no lugar dela vai um índice único
-- NULLS NOT DISTINCT (Postgres 15+), que mantém a unicidade tratando NULL como
-- valor. Nome: ux_mvt_nn_exigiveis_resumo (equivale ao pk_mvt_nn_exigiveis do §5.3).
--
-- Detalhes: quatro índices (keyset_eq, keyset_nf, keyset_ret, chave), com as
-- colunas INCLUDE do §5.3 para permitir Index Only Scan.

DO $$
DECLARE
    v_esquema text;
BEGIN
    FOREACH v_esquema IN ARRAY ARRAY['exigiveis_a', 'exigiveis_b']
    LOOP
        EXECUTE format($ddl$
            CREATE UNIQUE INDEX ux_mvt_nn_exigiveis_resumo
                ON %I.mvt_notas_nacionais_exigiveis
                (data_competencia_municipio, cpf_cnpj_responsavel, tipo_retencao, opcao_simples_nacional, retencao)
                NULLS NOT DISTINCT
            $ddl$, v_esquema);

        EXECUTE format($ddl$
            CREATE INDEX ix_mvt_nnex_det_keyset_eq
                ON %I.mvt_notas_nacionais_exigiveis_detalhes
                (cpf_cnpj_responsavel, data_competencia_municipio, tipo_retencao, nota_fiscal, nota_nacional)
                INCLUDE (chave_acesso, data_validacao, valor_issqn, valor_servico, cpf_cnpj_contraparte, nome_contraparte)
            $ddl$, v_esquema);

        EXECUTE format($ddl$
            CREATE INDEX ix_mvt_nnex_det_keyset_nf
                ON %I.mvt_notas_nacionais_exigiveis_detalhes
                (cpf_cnpj_responsavel, data_competencia_municipio, nota_fiscal, nota_nacional)
                INCLUDE (tipo_retencao, chave_acesso, data_validacao, valor_issqn, valor_servico, cpf_cnpj_contraparte, nome_contraparte)
            $ddl$, v_esquema);

        EXECUTE format($ddl$
            CREATE INDEX ix_mvt_nnex_det_keyset_ret
                ON %I.mvt_notas_nacionais_exigiveis_detalhes
                (cpf_cnpj_responsavel, data_competencia_municipio, cpf_cnpj_contraparte, nota_fiscal, nota_nacional)
                INCLUDE (tipo_retencao, chave_acesso, data_validacao, valor_issqn, valor_servico, nome_contraparte)
            $ddl$, v_esquema);

        EXECUTE format($ddl$
            CREATE INDEX ix_mvt_nnex_det_chave
                ON %I.mvt_notas_nacionais_exigiveis_detalhes
                (cpf_cnpj_responsavel, data_competencia_municipio, chave_acesso)
                INCLUDE (tipo_retencao, opcao_simples_nacional, retencao, valor_servico, valor_ded_red, valor_issqn, valor_base_calculo)
            $ddl$, v_esquema);
    END LOOP;
END
$$;


-- ===== 00004_publicacao_exigiveis.sql =====
-- Publicação atômica dos exigíveis: views public.* sobre o slot ativo, histórico
-- em public.publicacoes e a função publicar_exigiveis().
--
-- Fluxo: o carregador enche o slot inativo (exigiveis_a ou exigiveis_b) e chama
-- publicar_exigiveis(). A função confere as contagens, aponta as views para o
-- slot e registra a publicação, tudo na mesma transação: se algo falhar, nada muda.
--
-- publicar_exigiveis é SECURITY DEFINER (roda com os privilégios de quem aplicou
-- a migração, dono das views): o carregador não pode recriar views nem escrever
-- em publicacoes diretamente, só chamar a função. search_path fixo e todos os
-- nomes qualificados, para a função não ser sequestrada por objetos em
-- esquemas do chamador. EXECUTE é revogado de PUBLIC e concedido na 00008.

CREATE TABLE public.publicacoes (
    id             bigserial   PRIMARY KEY,
    slot           text        NOT NULL CHECK (slot IN ('a', 'b')),
    flow_run_id    text        NOT NULL,
    fonte          text        NOT NULL,
    linhas_resumo  bigint,
    linhas_detalhe bigint,
    checksum       text,
    publicado_em   timestamptz NOT NULL DEFAULT now(),
    ativo          boolean     NOT NULL DEFAULT false
);

-- No máximo uma publicação ativa.
CREATE UNIQUE INDEX ux_publicacoes_ativo ON public.publicacoes (ativo) WHERE ativo;

-- Estado inicial: as views leem o slot A.
CREATE VIEW public.mvt_notas_nacionais_exigiveis_detalhes AS
SELECT cpf_cnpj_responsavel, nome_contraparte, cpf_cnpj_contraparte, retencao,
       valor_servico, valor_ded_red, valor_issqn, valor_base_calculo,
       nota_fiscal, chave_acesso, data_validacao, data_competencia_municipio,
       tipo_retencao, nota_nacional, dps, opcao_simples_nacional
  FROM exigiveis_a.mvt_notas_nacionais_exigiveis_detalhes;

CREATE VIEW public.mvt_notas_nacionais_exigiveis AS
SELECT cpf_cnpj_responsavel, data_competencia_municipio, tipo_retencao,
       opcao_simples_nacional, retencao, quantidade_notas,
       total_valor_servico, total_valor_ded_red, total_valor_issqn,
       total_valor_base_calculo, cnt_valor_servico, cnt_valor_ded_red,
       cnt_valor_issqn, cnt_valor_base_calculo
  FROM exigiveis_a.mvt_notas_nacionais_exigiveis;

CREATE FUNCTION public.publicar_exigiveis(
    p_slot           text,
    p_flow_run_id    text,
    p_fonte          text,
    p_linhas_resumo  bigint,
    p_linhas_detalhe bigint,
    p_checksum       text
) RETURNS bigint
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
    -- Listas de colunas idênticas às das views criadas acima: CREATE OR REPLACE
    -- VIEW exige o mesmo nome, tipo e ordem de colunas.
    c_colunas_detalhe CONSTANT text :=
        'cpf_cnpj_responsavel, nome_contraparte, cpf_cnpj_contraparte, retencao, '
        'valor_servico, valor_ded_red, valor_issqn, valor_base_calculo, '
        'nota_fiscal, chave_acesso, data_validacao, data_competencia_municipio, '
        'tipo_retencao, nota_nacional, dps, opcao_simples_nacional';
    c_colunas_resumo CONSTANT text :=
        'cpf_cnpj_responsavel, data_competencia_municipio, tipo_retencao, '
        'opcao_simples_nacional, retencao, quantidade_notas, '
        'total_valor_servico, total_valor_ded_red, total_valor_issqn, '
        'total_valor_base_calculo, cnt_valor_servico, cnt_valor_ded_red, '
        'cnt_valor_issqn, cnt_valor_base_calculo';
    v_esquema         text;
    v_resumo          bigint;
    v_detalhe         bigint;
    v_id              bigint;
BEGIN
    IF p_slot IS NULL OR p_slot NOT IN ('a', 'b') THEN
        RAISE EXCEPTION 'slot inválido: %, esperado a ou b', coalesce(p_slot, 'NULL')
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    v_esquema := 'exigiveis_' || p_slot;

    -- Serializa publicações concorrentes até o fim da transação.
    PERFORM pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtext('public.publicar_exigiveis'));

    EXECUTE format('SELECT count(*) FROM %I.mvt_notas_nacionais_exigiveis', v_esquema) INTO v_resumo;
    EXECUTE format('SELECT count(*) FROM %I.mvt_notas_nacionais_exigiveis_detalhes', v_esquema) INTO v_detalhe;

    IF v_resumo IS DISTINCT FROM p_linhas_resumo OR v_detalhe IS DISTINCT FROM p_linhas_detalhe THEN
        RAISE EXCEPTION 'contagem divergente no slot %: resumo encontrado=% esperado=%, detalhe encontrado=% esperado=%',
            p_slot, v_resumo, coalesce(p_linhas_resumo::text, 'NULL'),
            v_detalhe, coalesce(p_linhas_detalhe::text, 'NULL')
            USING ERRCODE = 'data_exception';
    END IF;

    EXECUTE format(
        'CREATE OR REPLACE VIEW public.mvt_notas_nacionais_exigiveis_detalhes AS SELECT %s FROM %I.mvt_notas_nacionais_exigiveis_detalhes',
        c_colunas_detalhe, v_esquema);
    EXECUTE format(
        'CREATE OR REPLACE VIEW public.mvt_notas_nacionais_exigiveis AS SELECT %s FROM %I.mvt_notas_nacionais_exigiveis',
        c_colunas_resumo, v_esquema);

    UPDATE public.publicacoes SET ativo = false WHERE ativo;

    INSERT INTO public.publicacoes
        (slot, flow_run_id, fonte, linhas_resumo, linhas_detalhe, checksum, ativo)
    VALUES
        (p_slot, p_flow_run_id, p_fonte, p_linhas_resumo, p_linhas_detalhe, p_checksum, true)
    RETURNING id INTO v_id;

    RETURN v_id;
END
$$;

REVOKE ALL ON FUNCTION public.publicar_exigiveis(text, text, text, bigint, bigint, text) FROM PUBLIC;


-- ===== 00005_espelho_cdc.sql =====
-- Espelho CDC (Oracle -> Postgres): guias, guias_notas_nacionais, guias_receitas
-- e mv_contribuintes, com as colunas usadas pelas consultas de produção
-- (anexo-sql-producao.md: B1, B2, B3-B12, D6, D7) e pela MV MVT_GUIAS_ATIVAS
-- (nfs-e origin/staging:NFSE.DBL/Scripts/db/NFSE_OWNER/materialized_views/MVT_GUIAS_ATIVAS.sql).
--
-- O repositório não tem o DDL real dessas tabelas. TODOS os tipos e chaves
-- primárias abaixo são PROVISORIO e devem ser trocados pelo DDL real capturado
-- (ops/dba/03). Sem precisão conhecida usa-se numeric/varchar.
--
-- Toda tabela espelhada (as três primeiras) carrega os metadados do CDC:
--   _scn          SCN Oracle do evento aplicado
--   _commit_ts    instante de commit no Oracle
--   _aplicado_em  instante em que o aplicador gravou no Postgres
--
-- Colunas e consultas que as usam:
--   guias: guia (B1,B2,D7), inscricao (B1: G.INSCRICAO = C.INSCRICAO),
--     exercicio+mes (MVT_GUIAS_ATIVAS.DATA_COMPETENCIA), status (todas: IN (0,1)),
--     tipo (B1: TIPO IN (10,11)), guia_base (B1: GC.GUIA_BASE = G.GUIA),
--     data_quitacao, data_pagamento, valor_pago (MVT_GUIAS_ATIVAS)
--   guias_notas_nacionais: guia, nota_nacional, data_competencia (anti-join B2-B11),
--     tipo_iss (leitura da guia)
--   guias_receitas: guia, receita, quantidade_notas (B1, D6, D7: SUM(QUANTIDADE_NOTAS))
--   mv_contribuintes: inscricao, cpf_cnpj (B1: C.CPF_CNPJ = :par2), inscricao_notanacional,
--     pedagio, categoria, ativo
--
-- guias_notas_nacionais é particionada por mês de data_competencia
-- (2026-01 a 2030-12) mais uma partição DEFAULT.

CREATE TABLE public.guias (
    guia           numeric NOT NULL,                         -- PROVISORIO
    inscricao      varchar(15),                              -- PROVISORIO
    exercicio      numeric,                                  -- PROVISORIO
    mes            numeric,                                  -- PROVISORIO
    status         numeric,                                  -- PROVISORIO
    tipo           numeric,                                  -- PROVISORIO
    guia_base      numeric,                                  -- PROVISORIO
    data_quitacao  timestamp(0) without time zone,           -- PROVISORIO
    data_pagamento timestamp(0) without time zone,           -- PROVISORIO
    valor_pago     numeric,                                  -- PROVISORIO
    _scn           numeric,
    _commit_ts     timestamptz,
    _aplicado_em   timestamptz,
    -- PROVISORIO: substituir pelo DDL real capturado (ops/dba/03)
    CONSTRAINT pk_guias PRIMARY KEY (guia)
);

CREATE TABLE public.guias_notas_nacionais (
    guia             numeric NOT NULL,                       -- PROVISORIO
    nota_nacional    varchar(53) NOT NULL,                   -- PROVISORIO (53 = NOTAS_NACIONAIS.NOTA_NACIONAL)
    data_competencia timestamp(0) without time zone NOT NULL, -- PROVISORIO
    tipo_iss         numeric,                                -- PROVISORIO
    _scn             numeric,
    _commit_ts       timestamptz,
    _aplicado_em     timestamptz,
    -- PROVISORIO: substituir pelo DDL real capturado (ops/dba/03)
    CONSTRAINT pk_guias_notas_nacionais PRIMARY KEY (guia, nota_nacional, data_competencia)
) PARTITION BY RANGE (data_competencia);

DO $$
DECLARE
    v_mes date;
BEGIN
    FOR i IN 0..59
    LOOP
        v_mes := (DATE '2026-01-01' + make_interval(months => i))::date;
        EXECUTE format(
            'CREATE TABLE public.%I PARTITION OF public.guias_notas_nacionais FOR VALUES FROM (%L) TO (%L)',
            'guias_notas_nacionais_p' || to_char(v_mes, 'YYYYMM'),
            v_mes,
            (v_mes + INTERVAL '1 month')::date);
    END LOOP;
END
$$;

CREATE TABLE public.guias_notas_nacionais_pdefault
    PARTITION OF public.guias_notas_nacionais DEFAULT;

CREATE TABLE public.guias_receitas (
    guia             numeric NOT NULL,                       -- PROVISORIO
    receita          numeric NOT NULL,                       -- PROVISORIO
    quantidade_notas numeric,                                -- PROVISORIO
    _scn             numeric,
    _commit_ts       timestamptz,
    _aplicado_em     timestamptz,
    -- PROVISORIO: substituir pelo DDL real capturado (ops/dba/03)
    CONSTRAINT pk_guias_receitas PRIMARY KEY (guia, receita)
);

CREATE TABLE public.mv_contribuintes (
    inscricao              varchar(15) NOT NULL,             -- PROVISORIO
    cpf_cnpj               varchar(14),
    inscricao_notanacional varchar(15),                      -- PROVISORIO
    pedagio                numeric,                          -- PROVISORIO
    categoria              numeric,                          -- PROVISORIO
    ativo                  numeric,                          -- PROVISORIO
    _sincronizado_em       timestamptz,
    -- PROVISORIO: substituir pelo DDL real capturado (ops/dba/03)
    CONSTRAINT pk_mv_contribuintes PRIMARY KEY (inscricao)
);

-- Índices de guia-nacional/05-contrato-de-dados.md §5.3 para o espelho.

-- Anti-join NOT EXISTS de todas as consultas das classes B e C.
CREATE INDEX ix_gnn_nn_comp_guia
    ON public.guias_notas_nacionais (nota_nacional, data_competencia, guia);

-- Subconsulta "ca" de B1 (o §5.3 usa data_competencia, que no espelho é exercicio+mes).
CREATE INDEX ix_guias_inscricao_competencia_guia
    ON public.guias (inscricao, exercicio, mes, guia)
    INCLUDE (status, tipo, guia_base);

-- Apoio à view mvt_guias_ativas (status IN (0,1)) e aos joins por guia ativa.
CREATE INDEX ix_guias_guia_ativas
    ON public.guias (guia) WHERE status IN (0, 1);

-- NOT EXISTS (GC.GUIA_BASE = G.GUIA AND GC.STATUS IN (0,1)) de B1/D6/D7.
CREATE INDEX ix_guias_base_ativas
    ON public.guias (guia_base) WHERE status IN (0, 1);

-- Resolução CPF_CNPJ -> INSCRICAO em B1.
CREATE INDEX ix_mv_contribuintes_cpfcnpj
    ON public.mv_contribuintes (cpf_cnpj, inscricao);


-- ===== 00006_view_guias_ativas.sql =====
-- View public.mvt_guias_ativas: reproduz a MV NFSE_OWNER.MVT_GUIAS_ATIVAS do Oracle.
--
-- Oracle: TO_DATE(G.EXERCICIO || LPAD(G.MES, 2, '0') || '01', 'YYYYMMDD') AS DATA_COMPETENCIA
--         (tipo DATE -> timestamp(0)), NVL(G.VALOR_PAGO, 0), WHERE G.STATUS IN (0,1).
-- Fonte: nfs-e origin/staging:NFSE.DBL/Scripts/db/NFSE_OWNER/materialized_views/MVT_GUIAS_ATIVAS.sql
-- A coluna GUIA_ROWID da MV não existe no Postgres e não é lida pelas consultas.

CREATE VIEW public.mvt_guias_ativas AS
SELECT guia,
       make_date(exercicio::int, mes::int, 1)::timestamp(0) AS data_competencia,
       status,
       tipo,
       data_quitacao,
       data_pagamento,
       coalesce(valor_pago, 0) AS valor_pago
  FROM public.guias
 WHERE status IN (0, 1);


-- ===== 00007_cdc_controle.sql =====
-- Controle do CDC: marca d'água por fonte e registro de reconciliações.
--
-- cdc_marca_dagua: até onde cada fonte (guias, contribuintes, ...) foi aplicada;
-- o gateway lê para medir o atraso do espelho.
-- cdc_reconciliacao: histórico das correções feitas pelo reconciliador (ex.: vínculos
-- de guias de alto volume trazidos do Oracle antes de a guia ficar ativa).

CREATE TABLE public.cdc_marca_dagua (
    fonte        text PRIMARY KEY,
    scn          numeric,
    commit_ts    timestamptz,
    aplicado_em  timestamptz
);

CREATE TABLE public.cdc_reconciliacao (
    id           bigserial   PRIMARY KEY,
    guia         numeric,
    motivo       text,
    linhas       bigint,
    executado_em timestamptz NOT NULL DEFAULT now()
);


-- ===== 00008_permissoes.sql =====
-- Permissões dos papéis de grupo (criados na 00001). Nada é concedido a PUBLIC.
--
--   nc_gateway_ro     SELECT nas views public.*, no espelho, em mv_contribuintes,
--                     publicacoes e cdc_marca_dagua. Nunca escreve. Não acessa os
--                     slots direto: lê pelas views, que rodam com o dono.
--   nc_carregador     ALL nas tabelas dos slots exigiveis_a/b e EXECUTE em
--                     publicar_exigiveis. Nenhum acesso a public.publicacoes
--                     (a função, SECURITY DEFINER, escreve por ele) nem ao espelho.
--   nc_aplicador      SELECT/INSERT/UPDATE/DELETE no espelho, cdc_marca_dagua e
--                     cdc_reconciliacao.
--   nc_sincronizador  SELECT/INSERT/UPDATE/DELETE em mv_contribuintes e
--                     SELECT/INSERT/UPDATE em cdc_marca_dagua.

GRANT USAGE ON SCHEMA public TO nc_gateway_ro, nc_carregador, nc_aplicador, nc_sincronizador;
GRANT USAGE ON SCHEMA exigiveis_a, exigiveis_b TO nc_carregador;

-- nc_gateway_ro: somente leitura
GRANT SELECT ON
    public.mvt_notas_nacionais_exigiveis,
    public.mvt_notas_nacionais_exigiveis_detalhes,
    public.mvt_guias_ativas,
    public.guias,
    public.guias_notas_nacionais,
    public.guias_receitas,
    public.mv_contribuintes,
    public.publicacoes,
    public.cdc_marca_dagua
TO nc_gateway_ro;

-- nc_carregador: slots e publicação (o grant em lote cobre também as partições,
-- que são tabelas do schema)
GRANT ALL ON ALL TABLES IN SCHEMA exigiveis_a TO nc_carregador;
GRANT ALL ON ALL TABLES IN SCHEMA exigiveis_b TO nc_carregador;
GRANT EXECUTE ON FUNCTION public.publicar_exigiveis(text, text, text, bigint, bigint, text) TO nc_carregador;

-- nc_aplicador: espelho CDC
GRANT SELECT, INSERT, UPDATE, DELETE ON
    public.guias,
    public.guias_notas_nacionais,
    public.guias_receitas,
    public.cdc_marca_dagua,
    public.cdc_reconciliacao
TO nc_aplicador;
GRANT USAGE ON SEQUENCE public.cdc_reconciliacao_id_seq TO nc_aplicador;

-- nc_sincronizador: contribuintes
GRANT SELECT, INSERT, UPDATE, DELETE ON public.mv_contribuintes TO nc_sincronizador;
GRANT SELECT, INSERT, UPDATE ON public.cdc_marca_dagua TO nc_sincronizador;


-- ===== 00009_cdc_heartbeat_checkpoint.sql =====
-- Heartbeat na marca d'água e checkpoint do consumidor CDC.
--
-- Por que heartbeat: em período sem emissão de guias não chegam transações, e
-- commit_ts envelhece sem que o espelho esteja atrasado. O Debezium publica um
-- heartbeat a cada 5 s em cdc.nfse.heartbeat; como o stream é ordenado, aplicar um
-- heartbeat na sequência S significa que tudo publicado antes dele já foi aplicado.
-- O gateway considera o espelho defasado quando:
--   (a) now() - heartbeat_aplicado_em > limite (pipeline parado), ou
--   (b) a última transação foi aplicada há menos de "limite" e levou mais de
--       "limite" entre o commit no Oracle e a aplicação (pipeline lento).
-- Sem linha da fonte 'oracle', o espelho nunca foi alimentado: defasado.
--
-- Por que checkpoint: o aplicador consome o JetStream com consumidor ordenado e
-- grava a última sequência aplicada na MESMA transação Postgres dos dados. Ao
-- reiniciar, retoma em ultima_seq + 1. Assim a aplicação é efetivamente única
-- mesmo com entrega at-least-once, sem depender de ack do JetStream.

ALTER TABLE public.cdc_marca_dagua
    ADD COLUMN heartbeat_ts          timestamptz,
    ADD COLUMN heartbeat_aplicado_em timestamptz;

COMMENT ON COLUMN public.cdc_marca_dagua.scn IS 'commit_scn da última transação aplicada';
COMMENT ON COLUMN public.cdc_marca_dagua.commit_ts IS 'source.commit_ts_ms da última transação aplicada';
COMMENT ON COLUMN public.cdc_marca_dagua.aplicado_em IS 'momento do commit no Postgres da última transação aplicada';
COMMENT ON COLUMN public.cdc_marca_dagua.heartbeat_ts IS 'ts_ms do último heartbeat do Debezium aplicado em ordem';
COMMENT ON COLUMN public.cdc_marca_dagua.heartbeat_aplicado_em IS 'momento em que o último heartbeat foi aplicado';

CREATE TABLE public.cdc_checkpoint (
    stream        text        PRIMARY KEY,
    ultima_seq    bigint      NOT NULL,
    atualizado_em timestamptz NOT NULL DEFAULT now()
);

GRANT SELECT, INSERT, UPDATE ON public.cdc_checkpoint TO nc_aplicador;


-- ===== 00010_publicacoes_snapshot_bq.sql =====
-- Foto do BigQuery de cada publicação dos exigíveis: public.publicacoes.snapshot_bq e o 7º
-- parâmetro (opcional) de publicar_exigiveis.
--
-- Por quê: o Oracle e o Postgres recebem a mesma carga do BigQuery em momentos diferentes. A
-- pipeline de carga do Oracle grava, no COMENTÁRIO da tabela do slot (BQLOAD_*_A/_B), "snapshot do
-- BigQuery de <AAAA-MM-DD HH:MM:SS> UTC". O carregador do Postgres passa a mesma foto em
-- p_snapshot_bq, e o replayer de sombra (cmd/sombra replay) compara as duas antes e depois de cada
-- consulta: foto diferente é snapshot_mismatch, nunca divergência de dados.
--
-- Compatibilidade: p_snapshot_bq tem DEFAULT NULL, então as chamadas de 6 argumentos continuam
-- valendo (snapshot_bq fica NULL). Como a lista de parâmetros muda, a função de 6 argumentos é
-- removida e recriada (se as duas coexistissem, uma chamada de 6 argumentos seria ambígua). Mantém
-- SECURITY DEFINER, search_path fixo e o lock consultivo da 00004; o EXECUTE de PUBLIC é revogado
-- e o de nc_carregador, que a 00008 concedeu à assinatura antiga, é concedido de novo.

ALTER TABLE public.publicacoes ADD COLUMN snapshot_bq timestamptz;
COMMENT ON COLUMN public.publicacoes.snapshot_bq IS 'instante da foto do BigQuery carregada no slot (UTC); NULL se o carregador não informou';

DROP FUNCTION public.publicar_exigiveis(text, text, text, bigint, bigint, text);

CREATE FUNCTION public.publicar_exigiveis(
    p_slot           text,
    p_flow_run_id    text,
    p_fonte          text,
    p_linhas_resumo  bigint,
    p_linhas_detalhe bigint,
    p_checksum       text,
    p_snapshot_bq    timestamptz DEFAULT NULL
) RETURNS bigint
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
    -- Listas de colunas idênticas às das views criadas acima: CREATE OR REPLACE
    -- VIEW exige o mesmo nome, tipo e ordem de colunas.
    c_colunas_detalhe CONSTANT text :=
        'cpf_cnpj_responsavel, nome_contraparte, cpf_cnpj_contraparte, retencao, '
        'valor_servico, valor_ded_red, valor_issqn, valor_base_calculo, '
        'nota_fiscal, chave_acesso, data_validacao, data_competencia_municipio, '
        'tipo_retencao, nota_nacional, dps, opcao_simples_nacional';
    c_colunas_resumo CONSTANT text :=
        'cpf_cnpj_responsavel, data_competencia_municipio, tipo_retencao, '
        'opcao_simples_nacional, retencao, quantidade_notas, '
        'total_valor_servico, total_valor_ded_red, total_valor_issqn, '
        'total_valor_base_calculo, cnt_valor_servico, cnt_valor_ded_red, '
        'cnt_valor_issqn, cnt_valor_base_calculo';
    v_esquema         text;
    v_resumo          bigint;
    v_detalhe         bigint;
    v_id              bigint;
BEGIN
    IF p_slot IS NULL OR p_slot NOT IN ('a', 'b') THEN
        RAISE EXCEPTION 'slot inválido: %, esperado a ou b', coalesce(p_slot, 'NULL')
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    v_esquema := 'exigiveis_' || p_slot;

    -- Serializa publicações concorrentes até o fim da transação.
    PERFORM pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtext('public.publicar_exigiveis'));

    EXECUTE format('SELECT count(*) FROM %I.mvt_notas_nacionais_exigiveis', v_esquema) INTO v_resumo;
    EXECUTE format('SELECT count(*) FROM %I.mvt_notas_nacionais_exigiveis_detalhes', v_esquema) INTO v_detalhe;

    IF v_resumo IS DISTINCT FROM p_linhas_resumo OR v_detalhe IS DISTINCT FROM p_linhas_detalhe THEN
        RAISE EXCEPTION 'contagem divergente no slot %: resumo encontrado=% esperado=%, detalhe encontrado=% esperado=%',
            p_slot, v_resumo, coalesce(p_linhas_resumo::text, 'NULL'),
            v_detalhe, coalesce(p_linhas_detalhe::text, 'NULL')
            USING ERRCODE = 'data_exception';
    END IF;

    EXECUTE format(
        'CREATE OR REPLACE VIEW public.mvt_notas_nacionais_exigiveis_detalhes AS SELECT %s FROM %I.mvt_notas_nacionais_exigiveis_detalhes',
        c_colunas_detalhe, v_esquema);
    EXECUTE format(
        'CREATE OR REPLACE VIEW public.mvt_notas_nacionais_exigiveis AS SELECT %s FROM %I.mvt_notas_nacionais_exigiveis',
        c_colunas_resumo, v_esquema);

    UPDATE public.publicacoes SET ativo = false WHERE ativo;

    INSERT INTO public.publicacoes
        (slot, flow_run_id, fonte, linhas_resumo, linhas_detalhe, checksum, snapshot_bq, ativo)
    VALUES
        (p_slot, p_flow_run_id, p_fonte, p_linhas_resumo, p_linhas_detalhe, p_checksum, p_snapshot_bq, true)
    RETURNING id INTO v_id;

    RETURN v_id;
END
$$;

REVOKE ALL ON FUNCTION public.publicar_exigiveis(text, text, text, bigint, bigint, text, timestamptz) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION public.publicar_exigiveis(text, text, text, bigint, bigint, text, timestamptz) TO nc_carregador;

