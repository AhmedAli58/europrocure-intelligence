-- Q4: Top 20 vendors by total contract value 2018-2023
-- So what: Construction and energy dominate. Sweden appears 4x in top 10.
-- Vendor name inconsistency (EDF---Enedis) means concentration is understated.

SELECT
    VENDOR_NAME,
    VENDOR_COUNTRY,
    SUM(TOTAL_CONTRACTS)                        AS total_contracts,
    ROUND(SUM(TOTAL_VALUE_EUR) / 1e6, 2)        AS total_value_mn_eur,
    ROUND(100 * SUM(TOTAL_VALUE_EUR)
        / NULLIF(SUM(SUM(TOTAL_VALUE_EUR)) OVER (), 0), 4) AS pct_of_total_spend

FROM EUROPROCURE_DB.MARTS.MART_VENDOR_CONCENTRATION
WHERE VENDOR_NAME IS NOT NULL
    AND TOTAL_VALUE_EUR IS NOT NULL
GROUP BY VENDOR_NAME, VENDOR_COUNTRY
ORDER BY total_value_mn_eur DESC
LIMIT 20;
