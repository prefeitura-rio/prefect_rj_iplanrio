      SELECT *
      FROM `rj-smas-dev.pic.raw_cartao_primeira_infancia_carioca_bairros_entrega`
      WHERE
        lower(TRIM(APROVACAO_DISPARO_AVISO)) = "aprovado"
        AND DATE(DATA_DISPARO_AVISO) = CURRENT_DATE("America/Sao_Paulo")
    
