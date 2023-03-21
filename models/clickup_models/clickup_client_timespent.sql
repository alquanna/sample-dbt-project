WITH clean_clickup AS (
SELECT DISTINCT *
FROM {{source('clickup_adv', 'parent_clickup_task_timespent_daily')}}
)

SELECT
    name AS TaskName,
    user AS TaskCreator,
    foldername AS ClientFolder,
    EXTRACT(TIME from TIMESTAMP_MILLIS(time_spent)) AS TimeSpent,
    DATE(date_created) AS DateCreated,
    url AS TaskURL
FROM clean_clickup
{{ dbt_utils.group_by(n=6) }}