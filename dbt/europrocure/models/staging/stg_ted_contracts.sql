-- stg_ted_contracts.sql
-- Staging model for TED Contract Award Notices 2018-2023
-- Selects, renames, casts and filters the raw source data
-- No business logic here — that lives in intermediate models

with source as (
    select * from {{ source('raw', 'ted_contracts') }}
),

staged as (
    select
        -- Notice identifiers
        ID_NOTICE_CAN                           as notice_id,
        TED_NOTICE_URL                          as notice_url,
        ID_AWARD                                as award_id,
        ID_LOT_AWARDED                          as lot_id,

        -- Time dimensions
        YEAR                                    as pub_year,
        DT_DISPATCH                             as dispatch_date,
        DT_AWARD                                as award_date,
        AWARD_YEAR                              as award_year,
        AWARD_QUARTER                           as award_quarter,
        AWARD_MONTH                             as award_month,

        -- Buyer information
        ISO_COUNTRY_CODE                        as buyer_country,
        CAE_NAME                                as buyer_name,
        CAE_TYPE                                as buyer_type,
        CAE_TOWN                                as buyer_town,

        -- Contract details
        TYPE_OF_CONTRACT                        as contract_type,
        TOP_TYPE                                as procedure_type,
        CPV                                     as cpv_code,
        CPV_DIVISION                            as cpv_division,

        -- Values
        VALUE_EURO_FIN_2                        as contract_value_eur,
        AWARD_VALUE_EURO_FIN_1                  as award_value_eur,

        -- Vendor information
        WIN_NAME                                as vendor_name,
        WIN_COUNTRY_CODE                        as vendor_country,
        WIN_TOWN                                as vendor_town,

        -- Flags
        IS_SME                                  as is_sme,
        IS_FRAMEWORK                            as is_framework,
        B_EU_FUNDS                              as is_eu_funded,
        CANCELLED                               as is_cancelled,
        IS_FIRST_AWARD                          as is_first_award,

        -- Competition
        NUMBER_OFFERS                           as number_of_offers,
        COMPETITION_LEVEL                       as competition_level,

        -- Period classification
        COVID_PERIOD                            as covid_period,
        VALUE_BAND                              as value_band,

        -- Award criteria
        CRIT_CODE                               as award_criteria

    from source
    where CANCELLED != 1
        or CANCELLED is null
)

select * from staged