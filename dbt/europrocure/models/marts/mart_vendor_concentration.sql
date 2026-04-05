-- mart_vendor_concentration.sql
-- Vendor level aggregations for concentration analysis
-- Used for vendor concentration page
-- Key metric: what % of total spend goes to top N vendors

with enriched as (
    select * from {{ ref('int_contracts_enriched') }}
),

vendor_aggregated as (
    select
        vendor_name,
        vendor_country,
        award_year,
        covid_period,
        cpv_category_name,
        procedure_category,

        -- Volume
        count(notice_id)                as total_contracts,

        -- Value
        sum(contract_value_eur)         as total_value_eur,
        avg(contract_value_eur)         as avg_value_eur,

        -- SME status
        max(case when is_sme_bool = true then 1 else 0 end) as is_sme

    from enriched
    where vendor_name is not null
        and award_year between 2018 and 2023
    group by
        vendor_name,
        vendor_country,
        award_year,
        covid_period,
        cpv_category_name,
        procedure_category
)

select * from vendor_aggregated