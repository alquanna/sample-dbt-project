# DV360 Data Pull

SELECT
    day as Day,
    DATE_TRUNC(day, MONTH) as Month,
    DATE_TRUNC(day, WEEK) as Week,
    'DV360' as Platform,
    account_id AS AccountID,
    account_name AS AccountName,
    currency AS Currency,
    line_item_id AS CampaignID,
    '' AS Location,
    'Programmatic' AS CampaignType,
    line_item_name AS CampaignName,
    CASE 
    WHEN conversions_clickthrough > 0 THEN 'Click-through'
    WHEN conversions_viewthrough > 0 THEN 'View-through'
    ELSE ''
    END AS ConversionType,
    impressions AS Impressions,
    clicks AS Clicks,
    client_cost AS CostNative,
    conversions_clickthrough AS Conversions,
    0 AS ConversionValue
FROM {{source('dv360_adv', 'parent_dv360_lineitemperf_daily')}}
WHERE account_id IN (
    {%- for dv360 in var('dv360') -%}
    CAST({{- dv360 }} AS STRING)
    {%- if not loop.last -%},{% endif -%}
{% endfor %})