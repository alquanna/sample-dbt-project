{{ config(
    tags=["parked"]
) }}

SELECT ExternalCustomerId, acctName AccountDescriptiveName, AccountCurrencyCode
FROM (SELECT ExternalCustomerId, AccountCurrencyCode, LAST_VALUE(AccountDescriptiveName) OVER(PARTITION BY ExternalCustomerId ORDER BY _PARTITIONTIME asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) acctName
from {{source('gads', 'p_Customer_1513463703')}}
where ExternalCustomerId IN (
  {%- for gads in var('gads') -%}
    {{- gads -}}
    {%- if not loop.last -%},{% endif -%}
{% endfor -%})
GROUP BY 1, _PARTITIONTIME, AccountDescriptiveName, AccountCurrencyCode)
GROUP BY 1, 2, 3
ORDER BY 1 DESC