-- int_contracts_deduplicated.sql
-- Deduplicates the staging data at notice level
-- The raw data has ~4 rows per notice due to lot/award duplication
-- This model keeps one row per notice for notice-level analysis
-- Award-level analysis should use stg_ted_contracts directly

with staged as (
    select * from {{ ref('stg_ted_contracts') }}
),

deduplicated as (
    select
        notice_id,
        notice_url,
        pub_year,
        dispatch_date,
        award_date,
        award_year,
        award_quarter,
        award_month,
        buyer_country,
        buyer_name,
        buyer_type,
        buyer_town,
        contract_type,
        procedure_type,
        cpv_code,
        cpv_division,
        contract_value_eur,
        is_sme,
        is_framework,
        is_eu_funded,
        competition_level,
        number_of_offers,
        covid_period,
        value_band,
        award_criteria,
        vendor_name,
        vendor_country
    from staged
    where is_first_award = 'Y'
        and (is_cancelled != '1' or is_cancelled is null)
)

select * from deduplicated