{%- macro cast_to_string(column_name) -%}
    {%- if target.type in ('databricks','spark') -%}
        CAST({{ column_name }} AS STRING)
    {%- elif target.type == 'bigquery' -%}
        CAST({{ column_name }} AS STRING)
    {%- elif target.type == 'snowflake' -%}
        CAST({{ column_name }} AS VARCHAR)
    {%- elif target.type in ('postgres','redshift') -%}
        CAST({{ column_name }} AS VARCHAR)
    {%- else -%}
        CAST({{ column_name }} AS STRING)
    {%- endif -%}
{%- endmacro -%}
