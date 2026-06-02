{%- macro cast_to_int(column_name, kind='int') -%}
    {%- if target.type in ('databricks','spark') -%}
        {%- if kind == 'bigint' -%}
            CAST({{ column_name }} AS BIGINT)
        {%- else -%}
            CAST({{ column_name }} AS INT)
        {%- endif -%}
    {%- elif target.type == 'bigquery' -%}
        CAST({{ column_name }} AS INT64)
    {%- elif target.type == 'snowflake' -%}
        {%- if kind == 'bigint' -%}
            CAST({{ column_name }} AS NUMBER(38,0))
        {%- else -%}
            CAST({{ column_name }} AS INTEGER)
        {%- endif -%}
    {%- elif target.type in ('postgres','redshift') -%}
        {%- if kind == 'bigint' -%}
            CAST({{ column_name }} AS BIGINT)
        {%- else -%}
            CAST({{ column_name }} AS INTEGER)
        {%- endif -%}
    {%- else -%}
        CAST({{ column_name }} AS INT)
    {%- endif -%}
{%- endmacro -%}