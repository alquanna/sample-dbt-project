# Facebook Ads Base Adverity Pull

WITH clean_fbads AS (
SELECT DISTINCT *
FROM {{source('fbads_adv', 'parent_facebook_adperf_agegender_daily')}}
WHERE 
Day >= '2022-01-01'
AND account_id IN (
    {%- for fbads in var('fbads') -%}
    CAST({{- fbads }} AS STRING)
    {%- if not loop.last -%},{% endif -%}
{% endfor %}
)
)

SELECT
    Day,
    DATE_TRUNC(Day, Month) AS Month,
    DATE_TRUNC(Day, Week) AS Week,
    'Facebook Ads' AS Platform,
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
    SUM(actions) AS Conversions,
    SUM(conversion_value) AS ConversionValue
FROM clean_fbads
{{ dbt_utils.group_by(n=12) }}