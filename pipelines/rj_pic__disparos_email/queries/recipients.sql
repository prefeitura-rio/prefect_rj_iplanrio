SELECT *
FROM `{{ table }}`
{%- if emails %}
WHERE recipiente_email IN ({% for email in emails %}'{{ email }}'{% if not loop.last %}, {% endif %}{% endfor %})
{%- endif %}
