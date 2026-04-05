-- mart_sme_participation.sql
-- SME participation rates by country and year
-- Used for SME analysis in executive summary
-- Policy question: are SMEs getting fair access to public contracts?

with enriched as (
    select * from {{ ref('int_contracts_enriched') }}
),

aggregated as (
    select
        buyer_country,
        award_year,
        covid_period,
        cpv_category_name,
        procedure_category,

        -- Total contracts
        count(notice_id)                                        as total_contracts,

        -- SME breakdown
        count(case when is_sme_bool = true then 1 end)          as sme_contracts,
        count(case when is_sme_bool = false then 1 end)         as non_sme_contracts,
        count(case when is_sme_bool is null then 1 end)         as unknown_sme_contracts,

        -- SME value
        sum(case when is_sme_bool = true
            then contract_value_eur end)                        as sme_value_eur,
        sum(case when is_sme_bool = false
            then contract_value_eur end)                        as non_sme_value_eur,

        -- Total value
        sum(contract_value_eur)                                 as total_value_eur,

        -- SME rate
        round(
            100.0 * count(case when is_sme_bool = true then 1 end)
            / nullif(count(notice_id), 0),
        2)                                                      as sme_rate_pct

    from enriched
    where award_year between 2018 and 2023
        and buyer_country is not null
    group by
        buyer_country,
        award_year,
        covid_period,
        cpv_category_name,
        procedure_category
)

select * from aggregated