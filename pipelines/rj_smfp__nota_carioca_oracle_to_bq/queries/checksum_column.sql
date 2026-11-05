COUNT(`$column`) AS c_$column,
    CAST(SUM(CAST(`$column` AS BIGNUMERIC)) AS STRING) AS s_$column
