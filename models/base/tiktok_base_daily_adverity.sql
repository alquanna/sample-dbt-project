# Tiktok Ads Base Adverity Pull

SELECT
    Day,
    DATE_TRUNC(Day, Month) AS Month,
    DATE_TRUNC(Day, Week) AS Week,
    'Tiktok Ads' AS Platform,
    account_id AS AccountID,
    account_name AS AccountName,
    currency AS Currency,
    campaign_id AS CampaignID,
    TRIM(SPLIT(campaign_name,'|')[SAFE_OFFSET(0)]) AS Location,
    'Social' AS CampaignType,
    campaign_name AS CampaignName,
    '' AS ConversionType,
    SUM(impressions) AS Impressions,
    SUM(clicks) AS Clicks,
    SUM(costs) AS CostNative,
    SUM(conversions) AS Conversions,
    0 AS ConversionValue
FROM {{source('tiktok_adv', 'parent_tiktok_adperf_daily')}}
WHERE account_id IN (
    {%- for tiktok in var('tiktok') -%}
    CAST({{- tiktok }} AS STRING)
    {%- if not loop.last -%},{% endif -%}
{% endfor %})
{{ dbt_utils.group_by(n=12) }}