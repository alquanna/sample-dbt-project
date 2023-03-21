select * from {{ref('googleads_base_campaign_daily')}}
{%- if var('fbads')|length > 0 %}
union all
select * from {{ref('fbads_base_campaign_daily_adverity')}}
{%- endif -%}
{%- if var('linkedin')|length > 0 %}
union all
select * from {{ref('linkedin_base_daily_adverity')}}
{%- endif -%}
{%- if var('tiktok')|length > 0 %}
union all
select * from {{ref('tiktok_base_daily_adverity')}}
{%- endif -%}
{%- if var('dv360')|length > 0 %}
union all
select * from {{ref('dv360_base_campaign_daily_adverity')}}
{%- endif -%}
{%- if var('twitter')|length > 0 %}
union all
select * from {{ref('twitter_base_campaign_daily')}}
{%- endif -%}
{%- if var('bingads')|length > 0 %}
union all
select * from {{ref('bingads_base_campaign_daily')}}
{%- endif -%}
{%- if var('reddit')|length > 0 %}
union all
select * from {{ref('redditads_base_campaign_daily')}}
{%- endif -%}
{%- if var('quora')|length > 0 %}
union all
select * from {{ref('quoraads_base_campaign_daily')}}
{%- endif -%}
{%- if var('apple')|length > 0 %}
union all
select * from {{ref('appleads_base_campaign_daily')}}
{%- endif -%}
