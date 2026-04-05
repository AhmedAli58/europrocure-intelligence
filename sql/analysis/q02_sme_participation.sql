-- Q2: SME participation rate by year
-- So what: SME rate increased during COVID from ~52% to ~59%
-- Smaller contracts during COVID gave SMEs better access

SELECT
    AWARD_YEAR,
    COVID_PERIOD,
    SUM(TOTAL_CONTRACTS)                                AS total_contracts,
    SUM(SME_CONTRACTS)                                  AS sme_contracts,
    SUM(NON_SME_CONTRACTS)                              AS non_sme_contracts,
    ROUND(100 * SUM(SME_CONTRACTS)
        / NULLIF(SUM(SME_CONTRACTS) + SUM(NON_SME_CONTRACTS), 0), 2) AS sme_rate_pct

FROM EUROPROCURE_DB.MARTS.MART_SME_PARTICIPATION
WHERE AWARD_YEAR BETWEEN 2018 AND 2023
GROUP BY AWARD_YEAR, COVID_PERIOD
ORDER BY AWARD_YEAR;
