{% macro generate_schema_name(custom_schema_name, node) -%}
    {#-
        Override default dbt schema naming agar tidak double-prefix.
        Default dbt: <target.schema>_<custom_schema>  → "staging_staging", "staging_marts"
        Dengan macro ini:
          - Model tanpa +schema  → pakai target.schema (profil)
          - Model dengan +schema → pakai nilai custom_schema_name apa adanya

        Contoh:
          target.schema = "staging"
          model dengan +schema: marts   → skema "marts"
          model tanpa +schema           → skema "staging"
    -#}
    {%- set default_schema = target.schema -%}

    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}

{%- endmacro %}
