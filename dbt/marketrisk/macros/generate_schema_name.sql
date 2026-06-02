{#
    Override the default schema naming behaviour:
    
    Default dbt behaviour:
        - No custom schema: uses the default schema defined in the profile (e.g. 'silver')
        - Custome schema specified: concatenates default schema + custom schema (e.g. 'silver_gold')

    Our behaviour:
        - No custom schema → uses profile default (silver)  
        - Custom schema set → uses exactly that schema (gold, not silver_gold)
    
    This macro overrides the default behaviour to use the custom schema as-is without prefixing it with the default schema.
    This allows us to have clean, separate schemas for each layer (bronze, silver, gold) without the default schema as a prefix.

    To use this macro, simply specify the desired schema in the model config (e.g. schema='gold') and dbt will call this macro to determine the final schema name. 
    This is the standard approach for projects with multiple schemas.
#}

{% macro generate_schema_name(custom_schema, node) -%}
    {# If a custom schema is provided, use it as-is. Otherwise, fall back to the default schema from the profile. #}
    {% if custom_schema %}
        {{ return(custom_schema | trim) }}
    {% else %}
        {{ return(target.schema) }}
    {% endif %}
{%- endmacro %}