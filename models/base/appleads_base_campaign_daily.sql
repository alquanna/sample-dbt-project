SELECT 
    day as Day,
    DATE_TRUNC(day, MONTH) as Month,
    DATE_TRUNC(day, WEEK) as Week,
    'Quora' AS Platform,
    account_id as AccountID,
    account_name as AccountName,
    currency as Currency,
    campaign_id as CampaignID,
    '' as Location,
    'Social' as CampaignType,
    campaign_name as CampaignName,
    SUM(impressions) as Impressions,
    SUM(clicks) as Clicks,
    SUM(costs) as Cost,
    SUM(conversions) as TotalConversions,
    0 as ConversionValue
FROM {{source('apple_adv','parent_appleads_campaign_daily')}}
WHERE account_id IN (
    {%- for apple in var('apple') -%}
    '{{- apple }}'
    {%- if not loop.last -%},{% endif -%}
{% endfor %})
{{ dbt_utils.group_by(n=12) }}