SELECT NVL(SUM(alloc_bytes), 0) AS alloc_bytes
FROM gv$$inmemory_area
