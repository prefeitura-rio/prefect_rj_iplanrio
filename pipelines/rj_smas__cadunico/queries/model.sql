{{ "{{" }}
    config(
        alias='{{ alias }}',
        schema='protecao_social_cadunico',
        materialized="table",
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "month",
        }
    )
{{ "}}" }}

{%- macro blank(source) %}WHEN REGEXP_CONTAINS({{ source }}, r'^\s*$') THEN NULL{% endmacro %}
{%- for version in versions %}
SELECT
{%- for field in version.fields %}

    --column: {{ field.column }}
{%- if field.kind == "null" %}
    NULL AS {{ field.alias }}, --Essa coluna não esta na versao posterior
{%- if field.mapped_alias %}

    --column: {{ field.column }}
    NULL AS {{ field.mapped_alias }}, --Essa coluna não esta na versao posterior
{%- endif %}
{%- elif field.kind == "date" %}
    SAFE.PARSE_DATE(
        '{{ field.date_format }}',
        CASE
            {{ blank(field.source) }}
            ELSE TRIM({{ field.source }})
        END
    ) AS {{ field.alias }},
{%- elif field.kind == "int" %}
    SAFE_CAST(
        CASE
            {{ blank(field.source) }}
            ELSE TRIM({{ field.source }})
        END AS {{ field.type }}
    ) AS {{ field.alias }},
{%- elif field.kind == "float" %}
    SAFE_CAST(
        CASE
            {{ blank(field.source) }}
            ELSE SAFE_CAST(TRIM({{ field.source }}) AS INT64) / {{ field.divisor }}
        END AS {{ field.type }}
    ) AS {{ field.alias }},
{%- else %}
    CAST(
        CASE
            {{ blank(field.source) }}
            ELSE TRIM({{ field.source }})
        END AS {{ field.type }}
    ) AS {{ field.alias }},
{%- if field.mapped_alias %}

    --column: {{ field.column }}
    CAST(
        CASE
            {{ blank(field.source) }}
{%- for item in field.mapping %}
            WHEN REGEXP_CONTAINS({{ field.source }}, r'^{{ item.key }}$') THEN '{{ item.value }}'
{%- endfor %}
            ELSE TRIM({{ field.source }})
        END AS {{ field.type }}
    ) AS {{ field.mapped_alias }},
{%- endif %}
{%- endif %}
{%- endfor %}

    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `{{ project }}.{{ dataset }}_staging.{{ records }}`
WHERE versao_layout_particao = '{{ version.version }}'
    AND SUBSTRING(text,38,2) = '{{ number }}'
{%- if not loop.last %}

UNION ALL
{% endif %}
{%- endfor %}
