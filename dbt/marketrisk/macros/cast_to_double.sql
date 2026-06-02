{% macro cast_to_double(col) -%}
  {%- if target.type in ('databricks','spark') -%}
    CAST({{ col }} AS DOUBLE)
  {%- elif target.type == 'bigquery' -%}
    CAST({{ col }} AS FLOAT64)
  {%- elif target.type == 'snowflake' -%}
    CAST({{ col }} AS FLOAT)
  {%- elif target.type in ('postgres','redshift') -%}
    CAST({{ col }} AS double precision)
  {%- else -%}
    CAST({{ col }} AS DOUBLE)
  {%- endif -%}
{%- endmacro %}
