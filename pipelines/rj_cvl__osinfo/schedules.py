# -*- coding: utf-8 -*-

from iplanrio.pipelines_utils.prefect import create_dump_db_schedules


_osinfo_queries_weekly = [
    {
        "table_id": "usuario",
        "dataset_id": "adm_contrato_gestao",
        "dump_mode": "overwrite",
        "execute_query": """
            SELECT
                `COD_USUARIO`,
                `COD_UNIDADE`,
                `LOGIN`,
                `NOME`,
                `DATA_CADASTRO`,
                `DATA_ATUALIZACAO`,
                `DATA_EXCLUSAO`,
                `EXCLUIDO` as `FLG_EXCLUIDO`,
                `CARGO`
            FROM `adm_osinfo`.`usuario`;
        """,
    },
    {
        "table_id": "administracao_unidade",
        "dataset_id": "adm_contrato_gestao",
        "dump_mode": "overwrite",
        "execute_query": """
            SELECT
                `COD_UNIDADE`,
                `CNES`,
                `UNIDADE_FANTASIA` as `NOME_FANTASIA`,
                `SIGLA_TIPO`, `SIGLA_PERFIL`, `SIGLA_GESTAO`,
                `SIGLA_TIPO_GESTAO`, `RAZAO_SOCIAL`,
                `NOME_FANTA_ORIG` as `NOME_FANTASIA_ORIGINAL`,
                `UNIDADE_ABREVIADO`, `SIGLA`, `CNPJ`,
                `ENDERECO`,
                `ENDERECO_NUMERO` as `NUMERO`,
                `ENDERECO_COMPLEMENTO` as `COMPLEMENTO`,
                `BAIRRO`, `MUNICIPIO`,
                `CODMUNGEST` as `COD_MUNICIPIO`, `UF`, `CEP`,
                `REFERENCIA`,
                `TEL_DDD` as `TELELEFONE_DDD`,
                `TEL_1` as `TELEFONE_1`,
                `TEL_1_RAMAL` as `TELEFONE_1_RAMAL`,
                `TEL_2` as `TELEFONE_2`,
                `TEL_2_RAMAL` as `TELEFONE_2_RAMAL`, `FAX`,
                `TEL_SMS_CONSULTA` as `TELEFONE_SMS_CONSULTA`,
                `TEL_SMS_EXAME` as `TELEFONE_SMS_EXAME`, `EMAIL`,
                `DT_ULTIMA_ATUALIZ` as `DATA_ULTIMA_ATUALIZACAO`,
                `DATA_AVALIACAO_CONTADORES` as `DATA_AVALIACAO`
            FROM `adm_osinfo`.`adm_unidade`;
        """,
    },
    {
        "table_id": "usuario_sistema",
        "dataset_id": "adm_contrato_gestao",
        "dump_mode": "overwrite",
        "execute_query": """
            SELECT
                `ID_USUARIO_SISTEMA`,
                `COD_USUARIO`,
                `ID_SISTEMA`,
                `ID_PERFIL`,
                `DT_INICIAL` as `DATA_INICIAL`,
                `DT_FINAL` as `DATA_FINAL`
            FROM `adm_osinfo`.`usuario_sistema`;
        """,
    },
    {
        "table_id": "administracao_perfil",
        "dataset_id": "adm_contrato_gestao",
        "dump_mode": "overwrite",
        "execute_query": """
            SELECT
                `ID_PERFIL`,
                `NOME_PERFIL`
            FROM `adm_osinfo`.`adm_perfil`;
        """,
    },
    {
        "table_id": "administracao_unidade_perfil",
        "dataset_id": "adm_contrato_gestao",
        "dump_mode": "overwrite",
        "execute_query": """
            SELECT
                `ID_UNID_PERFIL` as `ID_UNIDADE_PERFIL`,
                `ID_USUARIO`,
                `COD_UNIDADE`,
                `ID_PERFIL`
            FROM `adm_osinfo`.`adm_unid_perfil`;
        """,
    },
    {
        "table_id": "tipo_documento",
        "dataset_id": "adm_contrato_gestao",
        "dump_mode": "overwrite",
        "execute_query": """
            SELECT
                `ID_TIPO_DOCUMENTO`,
                `COD_TIPO_DOCUMENTO` as `TIPO_DOCUMENTO`,
                `TIPO_DOCUMENTO` as `DOCUMENTO`
            FROM `osinfo`.`tipo_documento`;
        """,
    },
]


_osinfo_queries_daily = [
    {
        "table_id": "despesas",
        "dataset_id": "adm_contrato_gestao",
        "dump_mode": "overwrite",
        "execute_query": """
            SELECT
                `ID_DOCUMENTO`,
                `COD_OS` as `COD_ORGANIZACAO`,
                `COD_UNIDADE`, `DATA_ENVIO`,
                `ID_TIPO_DOCUMENTO`,
                `CODIGO_FISCAL`, `CNPJ`, `RAZAO`,
                `CPF`, `NOME`, `NUM_DOCUMENTO`,
                `SERIE`, `DESCRICAO`,
                `DATA_EMISSAO`, `DATA_VENCIMENTO`,
                `DATA_PAGAMENTO`, `DATA_APURACAO`,
                `VALOR_DOCUMENTO`, `VALOR_PAGO`,
                `ID_DESPESA`, `ID_RUBRICA`,
                `ID_CONTRATO`, `ID_CONTA_BANCARIA`,
                `REF_MES` as `REFERENCIA_MES`,
                `REF_ANO` as `REFERENCIA_ANO`,
                `COD_BANCARIO`,
                `flg_justificativa` as `FLG_JUSTIFICATIVA`,
                `PMT_MES` as `PARCELAMENTO_MES`,
                `PMT_TOTAL` as `PARCELAMENTO_TOTAL`,
                `ID_IMAGEM`,
                `nf_validada_sigma` as `NF_VALIDADA_SIGMA`,
                `dt_hora_validacao` as `DATA_VALIDACAO`
            FROM `osinfo`.`documento`;
        """,
        "materialize_after_dump": True,
    },
    {
        "table_id": "contrato",
        "dataset_id": "adm_contrato_gestao",
        "dump_mode": "overwrite",
        "execute_query": """
            SELECT
                `ID_CONTRATO`,
                `NUM_CONTRATO` as `NUMERO_CONTRATO`,
                `COD_OS` as `COD_ORGANIZACAO`,
                `DT_ATUALIZACAO` as `DATA_ATUALIZACAO`,
                `DT_ASSINATURA` as `DATA_ASSINATURA`,
                `PERIODO_VIGENCIA`,
                `DT_PUBLICACAO` as `DATA_PUBLICACAO`,
                `DT_INICIO` as `DATA_INICIO`,
                `VLR_TOTAL` as `VALOR_TOTAL`,
                `VLR_ANO1` as `VALOR_ANO1`,
                `VLR_PARCELAS` as `VALOR_PARCELAS`,
                `VLR_FIXO` as `VALOR_FIXO`,
                `VLR_VARIAVEL` as `VALOR_VARIAVEL`,
                `OBSERVACAO`,
                `AP`,
                `ID_SECRETARIA`
            FROM `osinfo`.`contrato`;
        """,
        "materialize_after_dump": True,
    },
    {
        "table_id": "plano_contas",
        "dataset_id": "adm_contrato_gestao",
        "dump_mode": "overwrite",
        "execute_query": """
            SELECT
                `ID_DESPESA`,
                `COD_DESPESA`,
                `DESPESA`,
                `N1` as `ID_DESPESA_N1`,
                `N2` as `ID_DESPESA_N2`,
                `FLG_ATIVO`
            FROM `osinfo`.`despesa`;
        """,
        "materialize_after_dump": True,
    },
    {
        "table_id": "rubrica",
        "dataset_id": "adm_contrato_gestao",
        "dump_mode": "overwrite",
        "execute_query": """
            SELECT
                `ID_RUBRICA`,
                `RUBRICA`,
                `FLG_ATIVO`
            FROM `osinfo`.`rubrica`;
        """,
        "materialize_after_dump": True,
    },
    {
        "table_id": "secretaria",
        "dataset_id": "adm_contrato_gestao",
        "dump_mode": "overwrite",
        "execute_query": """
            SELECT
                `ID_SECRETARIA`,
                `NOME_SECRETARIA` as `SECRETARIA`,
                `SIGLA_SECRETARIA` as `SIGLA`,
                `NOME_REGIONAL` as `REGIONAL`,
                `SIGLA_REGIONAL`,
                `COD_SECRETARIA`,
                `FLG_REGIONAL`
            FROM `osinfo`.`secretaria`;
        """,
        "materialize_after_dump": True,
    },
    {
        "table_id": "contrato_terceiros",
        "dataset_id": "adm_contrato_gestao",
        "dump_mode": "overwrite",
        "execute_query": """
            SELECT
                `ID_SERV_TER` as `ID_CONTRATO_TERCEIRO`,
                `COD_OS` as `CODIGO_ORGANIZACAO`,
                `ID_CONTRATO`, `VALOR_MES`,
                `CONTRATO_MES_INICIO`,
                `CONTRATO_ANO_INICIO`,
                `CONTRATO_MES_FIM`,
                `CONTRATO_ANO_FIM`,
                `COD_UNIDADE`,
                `REF_ANO` as `REFERENCIA_ANO_ASS_CONTRATO`,
                `VIGENCIA`, `CNPJ`, `RAZAO_SOCIAL`, `SERVICO`,
                `REF_MES` as `REFERENCIA_MES_RECEITA`,
                `ID_IMAGEM` as `FLG_IMAGEM`,
                `IMAGEM_CONTRATO`
            FROM `osinfo`.`serv_ter`;
        """,
        "materialize_after_dump": True,
    },
    {
        "table_id": "receita_dados",
        "dataset_id": "adm_contrato_gestao",
        "dump_mode": "overwrite",
        "execute_query": """
            SELECT
                `ID_RECEITA_DADOS`,
                `COD_UNIDADE`,
                `ID_RECEITA_ITEM` as `ID_ITEM`,
                `REF_MES` as `REFERENCIA_MES`,
                `REF_ANO` as `REFERENCIA_ANO`,
                `VALOR`,
                `FLG_ATIVO`,
                `ID_CONTRATO`,
                `ID_TERMO_ADITIVO`,
                `ID_CONTA_BANCARIA`
            FROM `osinfo`.`receita_dados`;
        """,
        "materialize_after_dump": True,
    },
    {
        "table_id": "receita_item",
        "dataset_id": "adm_contrato_gestao",
        "dump_mode": "overwrite",
        "execute_query": """
            SELECT
                `ID_RECEITA_ITEM`,
                `RECEITA_ITEM`,
                `FLG_ATIVO`,
                `ORDEM`,
                `ID_RECEITA_TIPO`
            FROM `osinfo`.`receita_item`;
        """,
        "materialize_after_dump": True,
    },
    {
        "table_id": "saldo_dados",
        "dataset_id": "adm_contrato_gestao",
        "dump_mode": "overwrite",
        "execute_query": """
            SELECT
                `ID_SALDO_DADOS`,
                `ID_SALDO_ITEM`,
                `REF_MES` as `REFERENCIA_MES_RECEITA`,
                `REF_ANO` as `REFERENCIA_ANO_RECEITA`,
                `VALOR`,
                `FLG_ATIVO`,
                `ID_CONTRATO`,
                `ID_CONTA_BANCARIA`,
                `NOME_ARQ_IMG_EXT` as `ARQ_IMG_EXT`
            FROM `osinfo`.`saldo_dados`;
        """,
        "materialize_after_dump": True,
    },
    {
        "table_id": "saldo_item",
        "dataset_id": "adm_contrato_gestao",
        "dump_mode": "overwrite",
        "execute_query": """
            SELECT
                `ID_SALDO_ITEM`,
                `SALDO_ITEM`,
                `FLG_ATIVO`,
                `ORDEM`,
                `ID_SALDO_TIPO`
            FROM `osinfo`.`saldo_item`;
        """,
        "materialize_after_dump": True,
    },
    {
        "table_id": "conta_bancaria",
        "dataset_id": "adm_contrato_gestao",
        "dump_mode": "overwrite",
        "execute_query": """
            SELECT
                `ID_CONTA_BANCARIA`,
                `ID_AGENCIA`,
                `CODIGO_CC`,
                `DIGITO_CC`,
                `FLG_ATIVO`,
                `COD_OS` as `COD_ORGANIZACAO`,
                `ID_CONTA_BANCARIA_TIPO` as `COD_TIPO`
            FROM `osinfo`.`conta_bancaria`;
        """,
        "materialize_after_dump": True,
    },
    {
        "table_id": "conta_bancaria_tipo",
        "dataset_id": "adm_contrato_gestao",
        "dump_mode": "overwrite",
        "execute_query": """
            SELECT
                `ID_CONTA_BANCARIA_TIPO`,
                `TIPO`,
                `SIGLA`
            FROM `osinfo`.`conta_bancaria_tipo`;
        """,
        "materialize_after_dump": True,
    },
    {
        "table_id": "agencia",
        "dataset_id": "adm_contrato_gestao",
        "dump_mode": "overwrite",
        "execute_query": """
            SELECT
                `agencia`.`ID_AGENCIA`,
                `agencia`.`ID_BANCO`,
                `agencia`.`CODIGO_AGENCIA` as `NUMERO_AGENCIA`,
                `agencia`.`DIGITO`,
                `agencia`.`NOME_AGENCIA` as `AGENCIA`,
                `agencia`.`FLG_ATIVO`
            FROM `osinfo`.`agencia`;
        """,
        "materialize_after_dump": True,
    },
    {
        "table_id": "banco",
        "dataset_id": "adm_contrato_gestao",
        "dump_mode": "overwrite",
        "execute_query": """
            SELECT
                `ID_BANCO`,
                `CODIGO_BANCO` as `COD_BANCO`,
                `NOME_BANCO` as `BANCO`,
                `NOME_FANTASIA_BANCO` as `NOME_FANTASIA`,
                `FLG_ATIVO`
            FROM `osinfo`.`banco`;
        """,
        "materialize_after_dump": True,
    },
    {
        "table_id": "itens_nota_fiscal",
        "dataset_id": "adm_contrato_gestao",
        "dump_mode": "overwrite",
        "execute_query": """
            SELECT
                `itnf_cd_item_nota_fiscal` as `ID_ITEM_NF`,
                `itnf_cd_item` as `COD_ITEM_NF`,
                `itnf_quantidade` as `QTD_MATERIAL`,
                `itnf_valor_unitario` as `VALOR_UNITARIO`,
                `itnf_ref_mes` as `REFERENCIA_MES_NF`,
                `itnf_ref_ano` as `REFERENCIA_ANO_NF`,
                `itnf_sq_fornecedor` as `ID_FORNECEDOR`,
                `itnf_valor_total` as `VALOR_TOTAL`,
                `itnf_num_documento` as `NUM_DOCUMENTO`,
                `cod_os` as `COD_ORGANIZACAO`,
                `data_envio` as `DATA_ENVIO`,
                `itnf_tipo` as `TIPO_ITEM`,
                `itnf_ds_item` as `ITEM`,
                `itnf_unidade_medida` as `UNIDADE_MEDIDA`,
                `itnf_observacao` as `OBSERVACAO`
            FROM `osinfo_V2`.`tb_itens_nota_fiscal`;
        """,
        "materialize_after_dump": True,
    },
    {
        "table_id": "fornecedor",
        "dataset_id": "adm_contrato_gestao",
        "dump_mode": "overwrite",
        "execute_query": """
            SELECT
                `sq_fornecedor` as `ID_FORNECEDOR`,
                `cd_fornecedor` as `COD_FORNECEDOR`,
                `ds_fornecedor` as `FORNECEDOR`,
                `in_tipo_pessoa` as `TIPO_PESSOA`,
                `forn_endereco` as `ENDERECO`,
                `forn_endereco_numero` as `NUMERO`,
                `forn_endereco_complemento` as `COMPLEMENTO`,
                `forn_endereco_cep` as `CEP`,
                `forn_endereco_bairro` as `BAIRRO`,
                `forn_endereco_municipio` as `MUNICIPIO`,
                `forn_endereco_UF` as `UF`,
                `forn_endereco_referencia` as `REFERENCIA`,
                `forn_telefone_1` as `TELEFONE_1`,
                `forn_telefone_1_ramal` as `TELEFONE_1_RAMAL`,
                `forn_telefone_2` as `TELEFONE_2`,
                `forn_telefone_2_ramal` as `TELEFONE_2_RAMAL`,
                `forn_telefone_fax` as `TELEFONE_FAX`,
                `forn_email` as `EMAIL`,
                `forn_pessoa_contato` as `CONTATO`,
                `id_imagem` as `ID_IMAGEM`,
                `id_log_importacao` as `ID_LOG_IMPORTACAO`,
                `cod_os` as `COD_ORGANIZACAO`,
                `data_envio` as `DATA_ENVIO`
            FROM `osinfo_V2`.`si_fornecedor`;
        """,
        "materialize_after_dump": True,
    },
    {
        "table_id": "historico_alteracoes",
        "dataset_id": "adm_contrato_gestao",
        "dump_mode": "overwrite",
        "execute_query": """
            SELECT
                `hial_id_historico_alteracoes` as `ID_HISTORICO_ALTERACOES`,
                `tipo_arquivo_id` as `ID_TIPO_ARQUIVO`,
                `cod_usuario` as `COD_USUARIO`,
                `cod_os` as `COD_ORGANIZACAO`,
                `hial_data_modificacao` as `DATA_MODIFICACAO`,
                `hial_valor_anterior` as `VALOR_ANTERIOR`,
                `hial_valor_novo` as `VALOR_NOVO`,
                `hial_mes_ref` as `MES_REFERENCIA`,
                `hial_ano_ref` as `ANO_REFERENCIA`,
                `hial_id_registro` as `ID_REGISTRO`,
                `hial_tipo_alteracao` as `TIPO_ALTERACAO`
            FROM `osinfo_V2`.`tb_historico_alteracoes`;
        """,
        "materialize_after_dump": True,
    },
    {
        "table_id": "tipo_arquivo",
        "dataset_id": "adm_contrato_gestao",
        "dump_mode": "overwrite",
        "execute_query": """
            SELECT
                `tipo_arquivo_id` as `ID_TIPO_ARQUIVO`,
                `descricao` as `TIPO_SERVICO`,
                `extensao_arquivo` as `EXTENSAO`,
                `estado` as `FLG_ATIVIDADE`
            FROM `osinfo_V2`.`tipo_arquivo`;
        """,
        "materialize_after_dump": True,
    },
    {
        "table_id": "fechamento",
        "dataset_id": "adm_contrato_gestao",
        "dump_mode": "overwrite",
        "execute_query": """
            SELECT
                `fmto_cd_fechamento` as `ID_FECHAMENTO`,
                `fmto_mes_referencia` as `MES_REFERENCIA`,
                `fmto_ano_referencia` as `ANO_REFERENCIA`,
                `fmto_cod_usuario` as `COD_USUARIO`,
                `fmto_data_inclusao` as `DATA_INCLUSAO`,
                `fmto_id_contrato` as `ID_CONTRATO`,
                `fmto_dt_limite` as `DATA_LIMITE`,
                `fmto_cod_os` as `COD_ORGANIZACAO`,
                `fmto_cd_estado_entrega` as `COD_ESTADO_ENTREGA`,
                `fmto_cd_tipo_entrega` as `COD_TIPO_ENTREGA`
            FROM `osinfo_V2`.`tb_fechamento`;
        """,
        "materialize_after_dump": True,
    },
    {
        "table_id": "estado_entrega",
        "dataset_id": "adm_contrato_gestao",
        "dump_mode": "overwrite",
        "execute_query": """
            SELECT
                `reen_cd_estado_entrega` as `COD_ESTADO`,
                `reen_ds_estado_entrega` as `ESTADO`,
                `reen_classe_css_estado_entrega` as `ETIQUETA`,
                `reen_ajuda_estado_entrega` as `DETALHE`
            FROM `osinfo_V2`.`tb_estado_entrega`;
        """,
        "materialize_after_dump": True,
    },
]
# The list of queries and their specific configurations

# General Deployment Settings
BASE_ANCHOR_DATE = "2025-07-15T00:00:00"
BASE_INTERVAL_SECONDS_DAILY = 3600 * 24  # Run each table every day
BASE_INTERVAL_SECONDS_WEEKLY = 3600 * 24 * 7 # Run each table every week
RUNS_SEPARATION_MINUTES = 5  # Stagger start times by 10 minutes
TIMEZONE = "America/Sao_Paulo"

# Database & Secret Settings
DB_TYPE = "sql_server"
DB_DATABASE = "REPLICA1746"
DB_HOST = "10.70.1.34"
DB_PORT = 1433
DATASET_ID = "brutos_1746"
INFISICAL_SECRET_PATH = "/db-1746"

# --- Generate and Print YAML ---
schedules_config = create_dump_db_schedules(
    table_parameters_list=_osinfo_queries_daily,
    base_interval_seconds=BASE_INTERVAL_SECONDS_DAILY,
    base_anchor_date_str=BASE_ANCHOR_DATE,
    runs_interval_minutes=RUNS_SEPARATION_MINUTES,
    timezone=TIMEZONE,
    db_type=DB_TYPE,
    db_database=DB_DATABASE,
    db_host=DB_HOST,
    db_port=DB_PORT,
    dataset_id=DATASET_ID,
    infisical_secret_path=INFISICAL_SECRET_PATH,
)

# Use sort_keys=False to maintain the intended order of keys in the output
print(schedules_config)

schedules_config_weekly = create_dump_db_schedules(
    table_parameters_list=_osinfo_queries_weekly,
    base_interval_seconds=BASE_INTERVAL_SECONDS_WEEKLY,
    base_anchor_date_str=BASE_ANCHOR_DATE,
    runs_interval_minutes=RUNS_SEPARATION_MINUTES,
    timezone=TIMEZONE,
    db_type=DB_TYPE,
    db_database=DB_DATABASE,
    db_host=DB_HOST,
    db_port=DB_PORT,
    dataset_id=DATASET_ID,
    infisical_secret_path=INFISICAL_SECRET_PATH,
)

# Use sort_keys=False to maintain the intended order of keys in the output
print(schedules_config_weekly)