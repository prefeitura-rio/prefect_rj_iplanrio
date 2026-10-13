{#
{
  "kind": "template",
  "description": "Reproject GeoJSON geometries to EPSG:4326 and write Parquet with WKT geometries.",
  "inputs": {
    "source": "Source Parquet path literal.",
    "dest": "Destination Parquet path literal.",
    "crs": "Source CRS literal.",
    "has_points": "Whether to add latitude and longitude columns."
  }
}
#}
-- noqa: disable=PRS
{% set geom -%}
ST_Transform(ST_GeomFromGeoJSON(geometry), {{ crs }}, 'EPSG:4326', always_xy := true)
{%- endset %}
COPY (
    SELECT
        * EXCLUDE (geometry),
        ST_AsText({{ geom }}) AS geometry
        {%- if has_points %},
        ST_Y({{ geom }}) AS latitude,
        ST_X({{ geom }}) AS longitude
        {%- endif %}
    FROM read_parquet({{ source }})
) TO {{ dest }} (FORMAT PARQUET)
