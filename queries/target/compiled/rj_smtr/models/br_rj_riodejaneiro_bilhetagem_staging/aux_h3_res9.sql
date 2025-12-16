

SELECT
  tile_id,
  ST_GEOGFROMTEXT(geometry) AS geometry
FROM
  `rj-smtr`.`br_rj_riodejaneiro_geo`.`h3_res9`