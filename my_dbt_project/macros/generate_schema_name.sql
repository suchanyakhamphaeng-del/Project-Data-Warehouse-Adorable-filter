{#
  ใช้ชื่อ schema ตามที่ config ไว้ตรง ๆ (เช่น +schema: marts -> schema `marts`)
  แทนพฤติกรรม default ของ dbt ที่จะต่อชื่อเป็น `staging_marts`
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
