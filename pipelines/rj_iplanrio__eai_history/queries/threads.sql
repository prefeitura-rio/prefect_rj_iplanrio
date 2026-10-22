SELECT DISTINCT ON (checkpoints.thread_id)
    checkpoints.thread_id,
    checkpoints.checkpoint_id,
    checkpoints.checkpoint->>'ts' AS ts,
    blobs.type AS blob_type,
    encode(blobs.blob, 'base64') AS blob_b64
FROM "public"."checkpoints" AS checkpoints
JOIN "public"."checkpoint_blobs" AS blobs
    ON blobs.thread_id = checkpoints.thread_id
    AND blobs.channel = 'messages'
    AND blobs.version = checkpoints.checkpoint->'channel_versions'->>'messages'
WHERE checkpoint_ts_immutable(checkpoints.checkpoint) > '{{ last_update }}'::timestamptz
ORDER BY checkpoints.thread_id, checkpoint_ts_immutable(checkpoints.checkpoint) DESC
{% if sql_limit %}LIMIT {{ sql_limit }}{% endif %}
