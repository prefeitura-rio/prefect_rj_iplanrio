"""Flow for rj_smfp__nota_carioca_parser.

Lê do índice ``documentos`` (escrito pelo extrator do ADN) do dataset do ambiente os NSUs acima da marca
``parser_marca(versao_parser, ultimo_nsu)``, abre os objetos NDJSON.gz no GCS, sanitiza, detecta a
versão, valida contra os XSDs oficiais (com as tolerâncias portadas do .NET) e grava por load job (nunca streaming) as
tabelas ``nfse``, ``dps``, ``pessoas`` e ``eventos``; o que não passa vai para ``quarentena`` com prioridade P1 a P5.
A marca só avança depois de todos os loads do lote, e repetir um lote não duplica (o id do load job é determinístico).
A cada nova ``versao_parser`` a quarentena é reavaliada e o que passa a ser válido é devolvido às tabelas.
Não há deduplicação aqui: NSU, NOTA_NACIONAL, DPS e EVENTO repetidos são a R-04, no dbt. Competência municipal,
responsável, cenário de exigibilidade e status da nota (R-01, R-02, R-06, R-08) também ficam no dbt.

Contrato raw (o dbt de queries-rj-iplanrio consome estes schemas; a fonte da verdade são os JSON de ``schemas_bq/``):

* nomes e valores iguais aos das tabelas do Oracle (``DFEN.NOTAS_NACIONAIS`` -> ``nfse``, ``DFEN.DPS`` -> ``dps``,
  ``DFEN.PESSOAS_NACIONAIS`` -> ``pessoas``, ``DFEN.EVENTOS_DFEN`` -> ``eventos``), com ``''`` gravado como NULL;
* mais ``NSU``, ``DATA_HORA_GERACAO`` (partição diária, do índice), ``VERSAO_PARSER`` e ``INGERIDO_EM``;
* ``PESSOA_EMITENTE``, ``PESSOA_PRESTADOR``, ``PESSOA_TOMADOR`` e ``PESSOA_INTERMEDIARIO`` guardam o ``HASH_ID`` (hex
  maiúsculo de 64 caracteres) da pessoa, não o GUID do Oracle; ``PESSOA_NACIONAL`` não existe no raw;
* ``pessoas`` tem uma linha por pessoa por NSU (a mesma pessoa em dois papéis aparece uma vez); a R-03 deduplica por
  ``HASH_ID`` no dbt;
* ``quarentena(nsu, prioridade, erro, versao_parser, avaliado_em)`` é histórico de avaliações: vale a mais recente de
  cada NSU, e ``prioridade`` nula significa devolvido (válido na ``versao_parser`` da linha).

nfse (35 colunas):
    NOTA_NACIONAL, CHAVE_ACESSO, NOTA_FISCAL, INSCRICAO_EMITENTE, PESSOA_EMITENTE, DESC_MUNICIPIO_EMISSOR,
    DESC_LOCAL_PRESTACAO, COD_LOCAL_INCIDENCIA, DESC_LOCAL_INCIDENCIA, DESC_TRIB_NACIONAL_ISSQN,
    DESC_TRIB_MUNICIPAL_ISSQN, DESC_NBS, TIPO_EMISSAO_DPS, MODO_EMISSAO_DPS, COD_STATUS_MSG, DATA_VALIDACAO,
    DPS, VALOR_DED_RED, BENEFICIO_NACIONAL, CALC_BM, VALOR_BASE_CALCULO, ALIQUOTA, VALOR_ISSQN,
    VALOR_TOTAL_RETENCOES, VALOR_LIQUIDO, DESCRICAO, CHAVE_ACESSO_SUBSTITUIDA, DFSE_SEQ, VERSAO,
    VERSAO_APLICACAO, AMBIENTE_GERADOR, NSU, DATA_HORA_GERACAO, VERSAO_PARSER, INGERIDO_EM

dps (72 colunas):
    DPS, VERSAO, TIPO_AMBIENTE, DATA_EMISSAO, VERSAO_APLICACAO, SERIE_DPS, NUMERO_DPS, DATA_COMPETENCIA,
    TIPO_EMITENTE, MUNICIPIO_EMISSOR, CHAVE_ACESSO_SUBSTITUIDA, MOTIVO_SUBSTITUICAO,
    JUSTIFICATIVA_SUBSTITUICAO, CHAVE_ACESSO_REJEITADA, MOTIVO_EMISSAO_TOMADOR_INTERM, PESSOA_PRESTADOR,
    PESSOA_TOMADOR, PESSOA_INTERMEDIARIO, LOCALIDADE_PRESTACAO, PAIS_PRESTACAO, OPCAO_CONSUMO_SERVICO,
    TRIBUTACAO_NACIONAL, TRIBUTACAO_MUNICIPAL, DESCRICAO_SERVICO, NBS, CODIGO_CONTRIBUINTE,
    RESPONSABILIDADE_TECNICA, DOC_REFERENCIA, INFORMACAO_COMPLEMENTAR, NUMERO_PEDIDO, GRUPO_SERVICO,
    SUBGRUPO_SERVICO, SERVICO_NACIONAL, VALOR_RECEBIDO, VALOR_SERVICO, VALOR_DESCONTO_INCONDICIONADO,
    VALOR_DESCONTO_CONDICIONADO, PERCENTUAL_DEDUCAO_REDUCAO, VALOR_DEDUCAO_REDUCAO, ISSQN, PAIS_RESULTADO,
    BENEFICIO_NACIONAL, BENEFICIO, VALOR_REDUCAO_BC_BENEFICIO, PERC_REDUCAO_BC_BENEFICIO, TIPO_SUSPENSAO,
    NUMERO_PROCESSO, TIPO_IMUNIDADE_ISSQN, ALIQUOTA, TIPO_RETENCAO_ISSQN, CST, VALOR_BASE_CALCULO_PIS_COFINS,
    ALIQUOTA_PIS, ALIQUOTA_COFINS, VALOR_PIS, VALOR_COFINS, TIPO_RETENCAO_PIS_COFINS, VALOR_CP, VALOR_IRRF,
    VALOR_CSLL, VALOR_TOTAL_TRIBUTO_FEDERAL, VALOR_TOTAL_TRIBUTO_ESTADUAL, VALOR_TOTAL_TRIBUTO_MUNICIPAL,
    PERC_TOTAL_TRIBUTO_FEDERAL, PERC_TOTAL_TRIBUTO_ESTADUAL, PERC_TOTAL_TRIBUTO_MUNICIPAL,
    INDICA_VALOR_TOTAL_TRIBUTOS, PERC_TRIBUTO_SIMPLES_NACIONAL, NSU, DATA_HORA_GERACAO, VERSAO_PARSER,
    INGERIDO_EM

pessoas (28 colunas):
    CPF_CNPJ, NIF, MOTIVO_NIF_NAO_INFORMADO, CAEPF, INSCRICAO, NOME, NOME_FANTASIA, MUNICIPIO_IBGE, CEP,
    LOGRADOURO, NUMERO, COMPLEMENTO, BAIRRO, UF, CODIGO_PAIS, CODIGO_POSTAL_EXTERIOR, LOCALIDADE_EXTERIOR,
    CIDADE_EXTERIOR, TELEFONE, EMAIL, OPCAO_SIMPLES_NACIONAL, REGIME_APURACAO_SIMPLES, REGIME_TRIBUTACAO,
    HASH_ID, NSU, DATA_HORA_GERACAO, VERSAO_PARSER, INGERIDO_EM

eventos (29 colunas):
    PEDIDO_EVENTO, CHAVE_ACESSO, TIPO_EVENTO, SEQUENCIAL_EVENTO, NUMERO_PEDIDO, DATA_EVENTO, DATA_REGISTRO,
    NUMERO_DFE, CODIGO_MOTIVO, MOTIVO_EVENTO, CPF_CNPJ_AUTOR_PEDIDO, CPF_AGENTE_TRIBUTARIO,
    CHAVE_ACESSO_SUBSTITUTA, NUMERO_PROCESSO_ADM, DESCRICAO_PROCESSO_ADM, TIPO_EVENTO_NFSE,
    EVENTO_BLOQUEIO_OFICIO, EVENTO_MANIFESTA_REJEICAO, AMBIENTE_GERADOR, TIPO_AMBIENTE, VERSAO_EVENTO,
    VERSAO_PEDIDO, VERSAO_APLICACAO_EVENTO, VERSAO_APLICACAO_PEDIDO, EVENTO, NSU, DATA_HORA_GERACAO,
    VERSAO_PARSER, INGERIDO_EM

Parâmetros: ``dataset_id`` e ``ambiente`` escolhem o dataset (índice, tabelas, marca, quarentena) e as entradas do
índice daquele ambiente (``prod`` ou ``staging``); ``max_lotes`` limita a execução (nulo vai até
esgotar o índice).

Kill switch: pause o deployment (os deployments nascem com a agenda INATIVA, até o gate dos todos 38 e 39).
"""

from iplanrio.pipelines_utils.env import inject_bd_credentials_task
from iplanrio.pipelines_utils.prefect import rename_current_flow_run_task
from prefect import flow

from pipelines.rj_smfp__nota_carioca_parser.constants import (
    AMBIENTE_PROD,
    DATASET_RAW,
    NSUS_POR_LOTE,
    PROJECT_ID,
    QUARENTENA_POR_EXECUCAO,
    VERSAO_PARSER,
)
from pipelines.rj_smfp__nota_carioca_parser.tasks import garantir_tabelas_task, processar_incremental_task
from pipelines.rj_smfp__nota_carioca_parser.utils.bigquery import DESDE_PADRAO


@flow(log_prints=True)
def rj_smfp__nota_carioca_parser(  # noqa: PLR0913, PLR0917
    dataset_id: str = DATASET_RAW,
    ambiente: str = AMBIENTE_PROD,
    project_id: str = PROJECT_ID,
    versao_parser: str = VERSAO_PARSER,
    nsus_por_lote: int = NSUS_POR_LOTE,
    max_lotes: int | None = None,
    reavaliar_quarentena: bool = True,
    quarentena_por_execucao: int = QUARENTENA_POR_EXECUCAO,
    desde: str = DESDE_PADRAO,
) -> None:
    rename_current_flow_run_task(new_name=f"{ambiente}-{versao_parser}")
    inject_bd_credentials_task(environment="prod")
    garantir_tabelas_task(project_id=project_id, dataset_id=dataset_id, ambiente=ambiente, desde=desde)
    processar_incremental_task(
        project_id=project_id,
        dataset_id=dataset_id,
        ambiente=ambiente,
        desde=desde,
        versao_parser=versao_parser,
        nsus_por_lote=nsus_por_lote,
        max_lotes=max_lotes,
        reavaliar_quarentena=reavaliar_quarentena,
        quarentena_por_execucao=quarentena_por_execucao,
    )
