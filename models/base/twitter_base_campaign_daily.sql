# Twitter Ads Base Adverity Pull

SELECT
    Day,
    DATE_TRUNC(Day, Month) AS Month,
    DATE_TRUNC(Day, Week) AS Week,
    'Twitter Ads' AS Platform,
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
    0 AS Conversions,
    0 AS ConversionValue
FROM {{source('twitter_adv', 'parent_twitterads_campaign_daily')}}
WHERE account_id IN (
    {%- for tw in var('twitter') -%}
    '{{- tw }}'
    {%- if not loop.last -%},{% endif -%}
{% endfor %})
{{ dbt_utils.group_by(n=12) }}