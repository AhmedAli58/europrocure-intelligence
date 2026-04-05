-- mart_procedure_trends.sql
-- Procedure type trends by year and country
-- Used for procedure openness analysis page
-- Core of the COVID impact hypothesis testing

with enriched as (
    select * from {{ ref('int_contracts_enriched') }}
),

aggregated as (
    select
        award_year,
        covid_period,
        buyer_country,
        procedure_type,
        procedure_category,

        -- Volume
        count(notice_id)                as total_contracts,
        sum(contract_value_eur)         as total_value_eur,
        avg(contract_value_eur)         as avg_value_eur,

        -- Competition
        avg(number_of_offers)           as avg_offers,
        count(case when number_of_offers = 1
            then 1 end)                 as single_bidder_contracts

    from enriched
    where award_year between 2018 and 2023
    group by
        award_year,
        covid_period,
        buyer_country,
        procedure_type,
        procedure_category
)

select * from aggregated