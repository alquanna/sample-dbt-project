# Google Ads conversion base

WITH clean_gadsconv AS (
SELECT DISTINCT *
FROM {{source('gads_adv', 'parent_googleads_campaign_conversionbreakdown_daily')}}
WHERE 
Day >= '2022-01-01'
AND account_id IN (
{%- for gads in var('gads') -%}
    CAST({{- gads }} AS STRING)
    {%- if not loop.last -%},{% endif -%}
{% endfor -%}
))


SELECT
  Day AS Day,
  DATE_TRUNC(Day, MONTH) AS Month,
  DATE_TRUNC(Day, WEEK) AS Week,
  'Google Ads' AS Platform,
  account_id AS AccountID,
  account_name AS AccountName,
  currency AS Currency,
  campaign_id AS CampaignID,
  TRIM(SPLIT(campaign_name,'|')[SAFE_OFFSET(0)]) AS Location,
  CASE
    WHEN REGEXP_CONTAINS(campaign_name,'Search|SEM') THEN 'Search'
    WHEN REGEXP_CONTAINS(campaign_name,'YouTube|Youtube|youtube') THEN 'YouTube'
    WHEN REGEXP_CONTAINS(campaign_name,'Gmail') THEN 'Gmail'
    WHEN REGEXP_CONTAINS(campaign_name,'Smart_Display') THEN 'Smart Display'
    WHEN REGEXP_CONTAINS(campaign_name,'Performance Max|Pmax|PMax|PMAX|P \\- Max') THEN 'PMax'
    WHEN REGEXP_CONTAINS(campaign_name,'Discovery') THEN 'Discovery'
    WHEN REGEXP_CONTAINS(campaign_name,'Shopping') THEN 'Shopping'
    WHEN REGEXP_CONTAINS(campaign_name,'Video') THEN 'Video'
    ELSE 'Others'
  END AS CampaignType,
  campaign_name as CampaignName,
  conversion_name as ConversionType,
  0 AS Impressions,
  0 AS Clicks,
  0 AS CostNative,
  SUM(conversions) as Conversions,
  SUM(conversion_value) as ConversionValue
FROM clean_gadsconv
{{ dbt_utils.group_by(n=12) }}