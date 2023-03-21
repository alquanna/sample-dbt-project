SELECT
   Day,
    DATE_TRUNC(Day, Month) AS Month,
    DATE_TRUNC(Day, Week) AS Week,
    'reddit Ads' AS Platform,
    account_id AS AccountID,
    account_name AS AccountName,
    'USD' AS Currency,
    campaign_id AS CampaignID,
    TRIM(SPLIT(campaign_name,'|')[SAFE_OFFSET(0)]) AS Location,
    'Social' AS CampaignType,
    campaign_name AS CampaignName,
    '' AS ConversionType,
    SUM(impressions) AS Impressions,
    SUM(clicks) AS Clicks,
    SUM(costs) AS Cost,
    0 as TotalConversions,
    0 as ConversionValue
FROM {{source('reddit_adv', 'parent_redditads_campaign_daily')}}
WHERE account_id IN (
    {%- for rd in var('reddit') -%}
    '{{- rd }}'
    {%- if not loop.last -%},{% endif -%}
{% endfor %})
{{ dbt_utils.group_by(n=12) }}