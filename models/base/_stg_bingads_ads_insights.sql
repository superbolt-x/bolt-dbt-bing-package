{{ config( 
        materialized='incremental',
        unique_key='unique_key'
) }}


{%- set schema_name, insights_table_name = 'bingads_raw', 'ad_performance_daily_report' -%}
{%- set insights_exclude_fields = [
   "ctr",
   "average_cpc",
   "conversion_rate",
   "cost_per_conversion",
   "return_on_ad_spend",
   "cost_per_assist",
   "revenue_per_conversion",
   "revenue_per_assist",
   "all_conversion_rate",
   "all_cost_per_conversion",
   "all_revenue_on_ad_spend",
   "all_revenue_per_conversion"
]
-%}

{%- set insights_fields = adapter.get_columns_in_relation(source(schema_name, insights_table_name))
                    |map(attribute="name")
                    |reject("in",insights_exclude_fields)
                    -%}  

WITH insights AS 
    (SELECT 
        {%- for field in insights_fields %}
        {{ get_bingads_clean_field(insights_table_name, field) }}
        {%- if not loop.last %},{%- endif %}
        {%- endfor %}
    FROM {{ source(schema_name, insights_table_name) }}
    )

SELECT *,
    MAX(_fivetran_synced) over (PARTITION BY account_id) as last_updated,
    ad_id||'_'||date as unique_key
FROM insights
{% if is_incremental() -%}

where date >= (select max(date)-30 from {{ this }})

{% endif %}