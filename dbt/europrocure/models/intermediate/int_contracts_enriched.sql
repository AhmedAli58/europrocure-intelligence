-- int_contracts_enriched.sql
-- Enriches the deduplicated contracts with derived analytical fields
-- This is where all business logic lives before aggregation into marts

with deduplicated as (
    select * from {{ ref('int_contracts_deduplicated') }}
),

enriched as (
    select
        -- All columns from deduplicated
        *,

        -- Contract value bands for analysis
        case
            when contract_value_eur is null then 'unknown'
            when contract_value_eur < 50000 then 'low'
            when contract_value_eur < 500000 then 'mid'
            when contract_value_eur < 5000000 then 'high'
            else 'very_high'
        end as value_band_derived,

        -- Procedure openness flag
        case
            when procedure_type = 'OPE' then 'open'
            when procedure_type in ('RES', 'NIC', 'NIP') then 'restricted'
            when procedure_type in ('NOC', 'NOP', 'AWP') then 'direct'
            when procedure_type in ('COD', 'INP') then 'negotiated'
            else 'other'
        end as procedure_category,

        -- SME flag as boolean
        case
            when is_sme = 'Y' then true
            when is_sme = 'N' then false
            else null
        end as is_sme_bool,

        -- Framework flag as boolean
        case
            when is_framework = 'Y' then true
            when is_framework = 'N' then false
            else null
        end as is_framework_bool,

        -- CPV division label
        case
            when cpv_division = '45' then 'Construction'
            when cpv_division = '72' then 'IT Services'
            when cpv_division = '79' then 'Business Services'
            when cpv_division = '71' then 'Architectural Services'
            when cpv_division = '33' then 'Medical Equipment'
            when cpv_division = '34' then 'Transport Equipment'
            when cpv_division = '90' then 'Waste Services'
            when cpv_division = '50' then 'Repair & Maintenance'
            when cpv_division = '48' then 'Software'
            when cpv_division = '60' then 'Transport Services'
            when cpv_division = '64' then 'Postal Services'
            when cpv_division = '85' then 'Health Services'
            when cpv_division = '44' then 'Construction Materials'
            when cpv_division = '55' then 'Hospitality Services'
            when cpv_division = '32' then 'Electronics'
            else 'Other'
        end as cpv_category_name

    from deduplicated
)

select * from enriched