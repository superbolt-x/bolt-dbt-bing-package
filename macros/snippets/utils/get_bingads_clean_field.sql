{%- macro get_bingads_clean_field(table_name, column_name) -%}

    {# /* Apply to all tables */ #}

    {%- if column_name in ('last_modified_time','modified_time') -%}
    {{column_name}} as updated_at
    
    {%- elif column_name == 'impressionsharepercent' -%}
    {{column_name}}/100 as impression_share
    
    {%- elif column_name == 'topimpressionratepercent' -%}
    SPLIT_PART({{column_name}},'%',1)::decimal/100 as top_impression_rate

    {%- elif 'keyword' in column_name -%}
    {{column_name}} as keyword_{{column_name.split('keyword')[1]}}

    {#- /* Apply to specific table */ -#}
    {%- elif "account" in table_name -%}

        {{column_name}} as account_{{column_name}}
    
    {%- elif "campaigns" in table_name -%}

        {%- if "_id" not in column_name  -%}
        {{column_name}} as campaign_{{column_name}}

        {%- else -%}
        {{column_name}}
        
        {%- endif -%}

    {%- elif "ad_groups" in table_name -%}

        {%- if "_id" not in column_name  -%}
        {{column_name}} as ad_group_{{column_name}}

        {%- else -%}
        {{column_name}}
        
        {%- endif -%}

    {%- elif "ads" in table_name -%}

        {%- if "_id" not in column_name  -%}
        {{column_name}} as ad_{{column_name}}

        {%- else -%}
        {{column_name}}
        
        {%- endif -%}

    {%- else -%}
    {{column_name}} 

    {%- endif -%}

{% endmacro -%}