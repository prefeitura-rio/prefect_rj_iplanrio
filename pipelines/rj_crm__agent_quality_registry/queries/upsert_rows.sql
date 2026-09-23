MERGE $target T
USING `$staging` S
ON T.$key = S.$key
WHEN MATCHED THEN
  UPDATE SET $updates
WHEN NOT MATCHED THEN
  INSERT ($inserts)
  VALUES ($values)
