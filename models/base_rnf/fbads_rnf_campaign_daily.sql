SELECT 
    day AS Day,
    account_id AS AccountID,
    account_name AS AccountName,
    campaign_id AS CampaignID,
    campaign_name AS CampaignName,
    reach AS Reach,
    ROUND(frequency, 2) AS Frequency
FROM {{source('fbads_adv', 'parent_facebook_campaignperf_rnf_daily')}}
WHERE 
day >= '2022-01-01'
 AND account_id IN (
    {%- for fbads in var('fbads') -%}
    CAST({{- fbads }} AS STRING)
    {%- if not loop.last -%},{% endif -%}
{% endfor %} 
 )