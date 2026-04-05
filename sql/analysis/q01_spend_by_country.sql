-- Q1: Top 15 countries by contract volume and total spend
-- So what: France leads at 19% of total spend. Germany has most contracts
-- but lower average value than France, Italy and UK.

SELECT
    BUYER_COUNTRY,
    SUM(TOTAL_CONTRACTS)                        AS total_contracts,
    ROUND(SUM(TOTAL_VALUE_EUR) / 1e9, 2)        AS total_spend_bn_eur,
    ROUND(AVG(AVG_VALUE_EUR), 0)                AS avg_contract_value_eur,
    ROUND(100 * SUM(TOTAL_VALUE_EUR)
        / SUM(SUM(TOTAL_VALUE_EUR)) OVER (), 2) AS pct_of_total_spend

FROM EUROPROCURE_DB.MARTS.MART_SPENDING_OVERVIEW
WHERE BUYER_COUNTRY IS NOT NULL
GROUP BY BUYER_COUNTRY
ORDER BY total_spend_bn_eur DESC
LIMIT 15;
