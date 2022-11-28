{%- macro get_bingads_default_campaign_types(campaign_name) -%}

 CASE 
    WHEN {{ campaign_name }} ~* 'search' AND {{ campaign_name }} ~* 'brand' AND {{ campaign_name }} !~* 'unbranded|nonbrand' THEN 'Campaign Type: Search Branded'
    WHEN {{ campaign_name }} ~* 'search' AND {{ campaign_name }} ~* 'unbranded|nonbrand' THEN 'Campaign Type: Search Nonbrand'
    WHEN {{ campaign_name }} ~* 'search' THEN 'Campaign Type: Search'
    WHEN {{ campaign_name }} ~* 'shopping' AND {{ campaign_name }} ~* 'brand' AND {{ campaign_name }} !~* 'unbranded|nonbrand' THEN 'Campaign Type: Shopping Branded'
    WHEN {{ campaign_name }} ~* 'shopping' AND {{ campaign_name }} ~* 'unbranded|nonbrand' THEN 'Campaign Type: Shopping Nonbrand'
    WHEN {{ campaign_name }} ~* 'shopping' THEN 'Campaign Type: Shopping'
    ELSE ''
    END AS campaign_type_default

{%- endmacro -%}