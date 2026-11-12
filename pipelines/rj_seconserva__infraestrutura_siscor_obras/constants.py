# -*- coding: utf-8 -*-
"""
Constantes e configurações para o pipeline de Infraestrutura SISCOR Obras - SECONSERVA.

Este módulo contém todas as configurações de tabelas e queries SQL
para o dump do banco de dados SISCOR (Sistema de Conservação e Obras).
"""

from dataclasses import dataclass
from enum import Enum


class Constants(Enum):
    """Constantes gerais do pipeline de Infraestrutura SISCOR Obras."""

    DATASET_ID = "infraestrutura_siscor_obras"
    DB_DATABASE = "siscor_seconserva"
    DB_HOST = "10.70.11.61"
    DB_PORT = "1433"
    DB_TYPE = "sql_server"
    INFISICAL_SECRET_PATH = "/db-siscor"


@dataclass
class TableConfig:
    """
    Configuração de uma tabela para dump.

    Attributes:
        table_id: Nome da tabela no BigQuery
        execute_query: Query SQL para extrair os dados
        dump_mode: Modo de dump ('overwrite' ou 'append')
        biglake_table: Se deve criar tabela BigLake
        materialize_after_dump: Se deve materializar após dump
        materialization_mode: Modo de materialização ('prod' ou 'dev')
        materialize_to_datario: Se deve materializar para DataRio
        dump_to_gcs: Se deve fazer dump para GCS
    """

    table_id: str
    execute_query: str
    dump_mode: str = "overwrite"
    biglake_table: bool = True
    materialize_after_dump: bool = True
    materialization_mode: str = "prod"
    materialize_to_datario: bool = False
    dump_to_gcs: bool = False


# Configurações de todas as tabelas do pipeline
TABLE_CONFIGS = {
    "processo_autorizacao_obra": TableConfig(
        table_id="processo_autorizacao_obra",
        execute_query="""
            SELECT
                distinct '26' as Secretaria,
                STR(po.nmr_processo, 6, 0) + '/' + po.ano_processo as Processo,
                po.sqnc_processo,
                case po.sqncl_tp_processo when 0 then tpr.tp_processo
                    else STR(po.sqncl_tp_processo,1)
                    + '° ' + tpr.tp_processo
                    END AS dscr_tp_processo,
                po.dt_protocolo,
                po.dt_inicio_obra,
                po.dt_fim_obra,
                po.area_ocupacao,
                po.observacao,
                po.prz_execucao,
                po.dt_aprovacao,
                nat.natureza_obra,
                tpo.tp_obra,
                lco.lcl_obra,
                vpfj.cpfcgc as 'Cgc_Requerente',
                vpfj.nome as requerente,
                lo.cdg_logradouro,
                lo.nm_lgrdr,
                po.dt_plenario,
                tps.dscr_situacao,
                tpp.dscr_parecer,
                po.nmr_licenca,
                pj.cgc as 'cgc_executor',
                pj.nm_fantasia as executor
            FROM PROCESSO_OCOR po
            INNER JOIN     NATUREZA_OBRA     nat ON
                po.cdg_natureza = nat.cdg_natureza
            INNER JOIN     TIPO_OBRA tpo         ON
                po.cdg_tp_obra = tpo.cdg_tp_obra
            INNER JOIN     TIPO_PROCESSO tpr    ON
                po.cdg_tp_processo = tpr.cdg_tp_processo
            INNER JOIN     TIPO_PARECER tpp    ON
                po.cdg_parecer = tpp.cdg_parecer
            INNER JOIN     TIPO_SITUACAO tps    ON
                po.cdg_situacao = tps.cdg_situacao
            INNER JOIN     LOCAL_OBRA lco        ON
                po.cdg_lcl_obra = lco.cdg_lcl_obra
            INNER join V_LOCALIZACAO lo on
                lo.nmr_processo = po.nmr_processo
                and lo.ano_processo = po.ano_processo
                left join requerente rp on
                rp.nmr_processo = po.nmr_processo
                and rp.ano_processo = po.ano_processo
                left join executor ex
                on ex.nmr_processo = po.nmr_processo
                and ex.ano_processo = po.ano_processo
                left join pessoa_juridica pj
                on pj.sqnc_pessoa = ex.sqnc_pessoa_executor
                left join v_ps_FscJrdc vpfj
                on vpfj.sqnc_pessoa = rp.sqnc_pessoa_requerente
            union
            SELECT
                distinct '06' as Secretaria,
                STR(po.nmr_processo, 6, 0) + '/' + po.ano_processo as Processo,
                po.sqnc_processo,
                case po.sqncl_tp_processo when 0 then tpr.tp_processo
                    else STR(po.sqncl_tp_processo,1)
                    + '° ' + tpr.tp_processo
                    END AS dscr_tp_processo,
                po.dt_protocolo,
                po.dt_inicio_obra,
                po.dt_fim_obra,
                po.area_ocupacao,
                po.observacao,
                po.prz_execucao,
                po.dt_aprovacao,
                nat.natureza_obra,
                tpo.tp_obra,
                lco.lcl_obra,
                vpfj.cpfcgc as 'Cgc_Requerente',
                vpfj.nome as requerente,
                lo.cdg_logradouro,
                lo.nm_lgrdr,
                po.dt_plenario,
                tps.dscr_situacao,
                tpp.dscr_parecer,
                po.nmr_licenca,
                pj.cgc as 'cgc_executor',
                pj.nm_fantasia as executor
            FROM siscor.dbo.PROCESSO_OCOR po
            INNER JOIN     siscor.dbo.NATUREZA_OBRA     nat ON
                po.cdg_natureza = nat.cdg_natureza
            INNER JOIN     siscor.dbo.TIPO_OBRA tpo         ON
                po.cdg_tp_obra = tpo.cdg_tp_obra
            INNER JOIN     siscor.dbo.TIPO_PROCESSO tpr    ON
                po.cdg_tp_processo = tpr.cdg_tp_processo
            INNER JOIN     siscor.dbo.TIPO_PARECER tpp    ON
                po.cdg_parecer = tpp.cdg_parecer
            INNER JOIN     siscor.dbo.TIPO_SITUACAO tps    ON
                po.cdg_situacao = tps.cdg_situacao
            INNER JOIN     siscor.dbo.LOCAL_OBRA lco        ON
                po.cdg_lcl_obra = lco.cdg_lcl_obra
                INNER join siscor.dbo.V_LOCALIZACAO lo on
                lo.nmr_processo = po.nmr_processo
                and lo.ano_processo = po.ano_processo
                left join siscor.dbo.requerente rp on
                rp.nmr_processo = po.nmr_processo
                and rp.ano_processo = po.ano_processo
                left join siscor.dbo.executor ex
                on ex.nmr_processo = po.nmr_processo
                and ex.ano_processo = po.ano_processo
                left join siscor.dbo.pessoa_juridica pj
                on pj.sqnc_pessoa = ex.sqnc_pessoa_executor
                left join siscor.dbo.v_ps_FscJrdc vpfj
                on vpfj.sqnc_pessoa = rp.sqnc_pessoa_requerente
        """,
    ),
}
