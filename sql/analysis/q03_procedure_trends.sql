-- Q3: Procedure type trends by year
-- So what: Direct awards jumped 40% in 2020 (7.4% to 10.4%)
-- This is the measurable COVID impact on procurement competition

SELECT
    AWARD_YEAR,
    PROCEDURE_CATEGORY,
    SUM(TOTAL_CONTRACTS)                                    AS total_contracts,
    ROUND(100 * SUM(TOTAL_CONTRACTS)
        / SUM(SUM(TOTAL_CONTRACTS)) OVER (PARTITION BY AWARD_YEAR), 2) AS pct_of_year

FROM EUROPROCURE_DB.MARTS.MART_PROCEDURE_TRENDS
WHERE AWARD_YEAR BETWEEN 2018 AND 2023
    AND PROCEDURE_CATEGORY IS NOT NULL
GROUP BY AWARD_YEAR, PROCEDURE_CATEGORY
ORDER BY AWARD_YEAR, total_contracts DESC;
