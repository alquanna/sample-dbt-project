# Bing (Microsoft) Ads data pull

SELECT
  Day AS Day,
  DATE_TRUNC(Day, MONTH) AS Month,
  DATE_TRUNC(Day, WEEK) AS Week,
  'Bing (Microsoft) Ads' AS Platform,
  account_id AS AccountID,
  account_name AS AccountName,
  currency AS Currency,
  campaign_id AS CampaignID,
  TRIM(SPLIT(campaign_name,'|')[SAFE_OFFSET(0)]) AS Location,
  'Search' AS CampaignType,
  campaign_name as CampaignName,
  '' as ConversionType,
  SUM(impressions) AS Impressions,
  SUM(clicks) AS Clicks,
  SUM(costs) AS CostNative,
  0 as Conversions,
  0 as ConversionValue
FROM {{source('bing_adv', 'parent_bingads_campaign_daily')}}
WHERE 
Day >= '2022-01-01'
AND account_id IN (
{%- for bingads in var('bingads') -%}
    '{{- bingads }}'
    {%- if not loop.last -%},{% endif -%}
{% endfor -%}
)
{{ dbt_utils.group_by(n=12) }}