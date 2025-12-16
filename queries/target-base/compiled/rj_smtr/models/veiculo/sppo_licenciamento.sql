-- depends_on: __dbt__cte__aux_sppo_licenciamento_vistoria_atualizada



  


with
     __dbt__cte__aux_sppo_licenciamento_vistoria_atualizada as (


/* Dados auxiliares de vistoria de ônibus levantados pela Coordenadoria Geral de Licenciamento e Fiscalização (TR/SUBTT/CGLF),
para atualização da data de última vistoria informada no sistema (STU). */

SELECT
  data,
  id_veiculo,
  placa,
  MAX(ano_ultima_vistoria) AS ano_ultima_vistoria,
FROM
    (
        SELECT
            data,
            id_veiculo,
            placa,
            ano_ultima_vistoria,
        FROM
            `rj-smtr`.`veiculo_staging`.`sppo_vistoria_tr_subtt_cglf_2023`
        UNION ALL
        SELECT
            data,
            id_veiculo,
            placa,
            ano_ultima_vistoria,
        FROM
            `rj-smtr`.`veiculo_staging`.`sppo_vistoria_tr_subtt_cglf_2024`
        UNION ALL
        SELECT
            data,
            id_veiculo,
            placa,
            ano_ultima_vistoria,
        FROM
            `rj-smtr`.`veiculo_staging`.`sppo_vistoria_tr_subtt_cglf_pendentes_2024`
    )
GROUP BY
  1,
  2,
  3
), -- Tabela de licenciamento
    stu as (
        select
            * except(data),
            date(data) AS data
        from
            `rj-smtr`.`veiculo_staging`.`sppo_licenciamento_stu` as t
        where
            date(data) = date("2023-02-01")
            and tipo_veiculo not like "%ROD%"
    ),
    stu_rn AS (
        select
            * except (timestamp_captura),
            EXTRACT(YEAR FROM data_ultima_vistoria) AS ano_ultima_vistoria,
            ROW_NUMBER() OVER (PARTITION BY data, id_veiculo) rn
        from
            stu
    ),
    stu_ano_ultima_vistoria AS (
        -- Temporariamente considerando os dados de vistoria enviados pela TR/SUBTT/CGLF
        
        SELECT
            s.* EXCEPT(ano_ultima_vistoria),
            s.ano_ultima_vistoria AS ano_ultima_vistoria_atualizado,
        FROM
            stu_rn AS s
        
    )
select
  * except(rn),
from
  stu_ano_ultima_vistoria
where
  rn = 1