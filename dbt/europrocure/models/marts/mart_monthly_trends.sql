-- mart_monthly_trends.sql
-- Monthly contract award trends 2018-2023
-- Used for time series analysis and COVID impact page
-- Shows volume and value of contracts awarded each month

with enriched as (
    select * from {{ ref('int_contracts_enriched') }}
),

aggregated as (
    select
        award_year,
        award_month,
        award_quarter,
        covid_period,
        buyer_country,
        procedure_category,
        contract_type,

        -- Volume
        count(notice_id)                as total_contracts,

        -- Value
        sum(contract_value_eur)         as total_value_eur,
        avg(contract_value_eur)         as avg_value_eur,
        median(contract_value_eur)      as median_value_eur,

        -- Competition
        avg(number_of_offers)           as avg_offers,

        -- SME
        count(case when is_sme_bool = true
            then 1 end)                 as sme_contracts

    from enriched
    where award_year between 2018 and 2023
        and award_month is not null
        and award_year is not null
    group by
        award_year,
        award_month,
        award_quarter,
        covid_period,
        buyer_country,
        procedure_category,
        contract_type
)

select * from aggregated