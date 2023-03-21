select 
    DATE_TRUNC(Day, MONTH) AS Month,
    account_id AS AccountID,
    currency AS Currency,
    'Google Ads' AS Platform,
    SUM(costs) AS Cost
from {{source('gads_adv', 'parent_googleads_campaign_daily')}}
    WHERE Day BETWEEN DATE_TRUNC(DATE_SUB(CURRENT_DATE('Asia/Singapore'), INTERVAL 1 MONTH), MONTH) 
    AND DATE_SUB(DATE_TRUNC(CURRENT_DATE('Asia/Singapore'), MONTH), INTERVAL 1 DAY)
    {{ dbt_utils.group_by(n=4) }}
union all
select 
    DATE_TRUNC(Day, MONTH) AS Month,
    account_id AS AccountID,
    currency AS Currency,
    'Facebook Ads' AS Platform,
    SUM(costs) AS Cost
from {{source('fbads_adv', 'parent_facebook_adperf_agegender_daily')}}
    WHERE Day BETWEEN DATE_TRUNC(DATE_SUB(CURRENT_DATE('Asia/Singapore'), INTERVAL 1 MONTH), MONTH) 
    AND DATE_SUB(DATE_TRUNC(CURRENT_DATE('Asia/Singapore'), MONTH), INTERVAL 1 DAY)
    {{ dbt_utils.group_by(n=4) }}
union all
select 
    DATE_TRUNC(Day, MONTH) AS Month,
    account_id AS AccountID,
    currency AS Currency,
    'LinkedIn Ads' AS Platform,
    SUM(costs) AS Cost
from {{source('linkedin_adv', 'parent_linkedin_adperf_creative_daily')}}
    WHERE Day BETWEEN DATE_TRUNC(DATE_SUB(CURRENT_DATE('Asia/Singapore'), INTERVAL 1 MONTH), MONTH) 
    AND DATE_SUB(DATE_TRUNC(CURRENT_DATE('Asia/Singapore'), MONTH), INTERVAL 1 DAY)
    {{ dbt_utils.group_by(n=4) }}
union all
select 
    DATE_TRUNC(Day, MONTH) AS Month,
    account_id AS AccountID,
    currency AS Currency,
    'Tiktok Ads' AS Platform,
    SUM(costs) AS Cost
from {{source('tiktok_adv', 'parent_tiktok_adperf_daily')}}
    WHERE Day BETWEEN DATE_TRUNC(DATE_SUB(CURRENT_DATE('Asia/Singapore'), INTERVAL 1 MONTH), MONTH) 
    AND DATE_SUB(DATE_TRUNC(CURRENT_DATE('Asia/Singapore'), MONTH), INTERVAL 1 DAY)
    {{ dbt_utils.group_by(n=4) }}
union all
select 
    DATE_TRUNC(Day, MONTH) AS Month,
    account_id AS AccountID,
    currency AS Currency,
    'DV360' AS Platform,
    SUM(media_cost) AS Cost
from {{source('dv360_adv', 'parent_dv360_lineitemperf_daily')}}
    WHERE Day BETWEEN DATE_TRUNC(DATE_SUB(CURRENT_DATE('Asia/Singapore'), INTERVAL 1 MONTH), MONTH) 
    AND DATE_SUB(DATE_TRUNC(CURRENT_DATE('Asia/Singapore'), MONTH), INTERVAL 1 DAY)
    {{ dbt_utils.group_by(n=4) }}
union all
select 
    DATE_TRUNC(Day, MONTH) AS Month,
    account_id AS AccountID,
    currency AS Currency,
    'Twitter Ads' AS Platform,
    SUM(costs) AS Cost
from {{source('twitter_adv', 'parent_twitterads_campaign_daily')}}
    WHERE Day BETWEEN DATE_TRUNC(DATE_SUB(CURRENT_DATE('Asia/Singapore'), INTERVAL 1 MONTH), MONTH) 
    AND DATE_SUB(DATE_TRUNC(CURRENT_DATE('Asia/Singapore'), MONTH), INTERVAL 1 DAY)
    {{ dbt_utils.group_by(n=4) }}
union all
select 
    DATE_TRUNC(Day, MONTH) AS Month,
    account_id AS AccountID,
    currency AS Currency,
    'Microsoft (Bing) Ads' AS Platform,
    SUM(costs) AS Cost
from {{source('bing_adv', 'parent_bingads_campaign_daily')}}
    WHERE Day BETWEEN DATE_TRUNC(DATE_SUB(CURRENT_DATE('Asia/Singapore'), INTERVAL 1 MONTH), MONTH) 
    AND DATE_SUB(DATE_TRUNC(CURRENT_DATE('Asia/Singapore'), MONTH), INTERVAL 1 DAY)
    {{ dbt_utils.group_by(n=4) }}
union all
select 
    DATE_TRUNC(Day, MONTH) AS Month,
    account_id AS AccountID,
    currency AS Currency,
    'reddit Ads' AS Platform,
    SUM(costs) AS Cost
from {{source('reddit_adv', 'parent_redditads_campaign_daily')}}
    WHERE Day BETWEEN DATE_TRUNC(DATE_SUB(CURRENT_DATE('Asia/Singapore'), INTERVAL 1 MONTH), MONTH) 
    AND DATE_SUB(DATE_TRUNC(CURRENT_DATE('Asia/Singapore'), MONTH), INTERVAL 1 DAY)
    {{ dbt_utils.group_by(n=4) }}
union all
select 
    DATE_TRUNC(Day, MONTH) AS Month,
    account_id AS AccountID,
    currency AS Currency,
    'Quora Ads' AS Platform,
    SUM(costs) AS Cost
from {{source('quora_adv', 'parent_quora_adlevel_daily')}}
    WHERE Day BETWEEN DATE_TRUNC(DATE_SUB(CURRENT_DATE('Asia/Singapore'), INTERVAL 1 MONTH), MONTH) 
    AND DATE_SUB(DATE_TRUNC(CURRENT_DATE('Asia/Singapore'), MONTH), INTERVAL 1 DAY)
    {{ dbt_utils.group_by(n=4) }}
union all
select 
    DATE_TRUNC(Day, MONTH) AS Month,
    account_id AS AccountID,
    currency AS Currency,
    'Quora Ads' AS Platform,
    SUM(costs) AS Cost
from {{source('apple_adv', 'parent_appleads_campaign_daily')}}
    WHERE Day BETWEEN DATE_TRUNC(DATE_SUB(CURRENT_DATE('Asia/Singapore'), INTERVAL 1 MONTH), MONTH) 
    AND DATE_SUB(DATE_TRUNC(CURRENT_DATE('Asia/Singapore'), MONTH), INTERVAL 1 DAY)
    {{ dbt_utils.group_by(n=4) }}