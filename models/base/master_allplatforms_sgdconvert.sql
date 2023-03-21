{{ config(
    tags=["parked"]
) }}

select
a.Day,
a.Month,
a.Week,
a.Platform,
a.AccountID,
a.AccountName,
a.Currency,
cur.Cost as ConvRate,
a.CampaignID,
a.Location,
a.CampaignType,
a.CampaignName,
a.ConversionType,
SUM(a.CostNative) AS CostNative,
SUM(a.Impressions) AS Impressions,
SUM(a.Clicks) AS Clicks,
SUM(a.Actions) AS Actions,
SUM(a.Cost)*cur.Cost as CostSGD
from {{ref('master_allplatforms_daily')}} a
left join {{source('currency_convert', 'sgd_conversion')}} cur
    on a.currency = cur.currency
{{ dbt_utils.group_by(n=13) }}