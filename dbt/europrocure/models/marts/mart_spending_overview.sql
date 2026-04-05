-- mart_spending_overview.sql
-- Total spending by country and year
-- Primary mart for executive summary and country analysis pages

with enriched as (
    select * from {{ ref('int_contracts_enriched') }}
),

aggregated as (
    select
        buyer_country,
        award_year,
        covid_period,
        contract_type,
        procedure_category,
        cpv_category_name,

        -- Volume metrics
        count(notice_id)                            as total_contracts,

        -- Value metrics
        sum(contract_value_eur)                     as total_value_eur,
        avg(contract_value_eur)                     as avg_value_eur,
        median(contract_value_eur)                  as median_value_eur,

        -- Competition metrics
        avg(number_of_offers)                       as avg_offers,

        -- SME metrics
        count(case when is_sme_bool = true then 1 end)  as sme_contracts,
        count(case when is_sme_bool = false then 1 end) as non_sme_contracts,

        -- Framework metrics
        count(case when is_framework_bool = true then 1 end) as framework_contracts

    from enriched
    where award_year between 2018 and 2023
        and buyer_country is not null
    group by
        buyer_country,
        award_year,
        covid_period,
        contract_type,
        procedure_category,
        cpv_category_name
)

select * from aggregated