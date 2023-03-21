SELECT 
    stg.Month,
    list.ClientName,
    stg.Platform,
    stg.Currency,
    SUM(Cost) AS Cost
FROM {{ref('monthly_cost_staging')}} stg
LEFT JOIN {{source('sample_clients', 'gsheet_sampleclients')}} list
ON stg.AccountID = list.AccountID
WHERE list.ClientName IS NOT NULL
AND list.MonthlyCostList = true
GROUP BY 1, 2, 3, 4
ORDER BY 1, 2, 3, 4