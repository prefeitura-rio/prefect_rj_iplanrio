    SELECT
      id_documento,
      num_documento,
      COALESCE(cnpj, cpf) AS cnpj_cpf,
      descricao_limpa,
      data_emissao,
      valor_documento,
      SUM(valor_pago)
        OVER (
          PARTITION BY num_documento, COALESCE(cnpj, cpf)
        ) AS valor_pago_total
    FROM `rj-nf-agent.poc_osinfo_ia.osinfo_despesas_recorte`
