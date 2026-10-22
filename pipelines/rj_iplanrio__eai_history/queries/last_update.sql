SELECT MAX(last_update) AS last_update
FROM `{{ table }}`
WHERE environment = '{{ environment }}'
    AND last_update IS NOT NULL
    AND last_update != 'None'
