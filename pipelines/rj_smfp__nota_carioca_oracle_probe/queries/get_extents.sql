SELECT o.data_object_id, e.relative_fno, e.block_id, e.blocks
FROM dba_extents e
JOIN dba_objects o
  ON o.owner = e.owner
 AND o.object_name = e.segment_name
 AND NVL(o.subobject_name, '-') = NVL(e.partition_name, '-')
 AND o.object_type = e.segment_type
WHERE e.owner = :owner
  AND e.segment_name = :table_name
  AND e.segment_type IN ('TABLE', 'TABLE PARTITION', 'TABLE SUBPARTITION')
  AND o.data_object_id IS NOT NULL
ORDER BY o.data_object_id, e.relative_fno, e.block_id
