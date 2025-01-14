with

    stateprovince as (
        select
            stateprovinceid
            , stateprovince_name
            , territoryid
        from {{ ref("stg_sap_adw__stateprovince") }}
    )

    , countryregion as (
        select
            countryregioncode
            , country_region_name
        from {{ ref("stg_sap_adw__countryregion") }}
    )

    , address as (
        select
            addressid
            , city
        from {{ ref("stg_sap_adw__address")}}
    )

    dim_territory as (
        select
            {{ dbt_utils.generate_surrogate_key(['stateprovinceid', 'countryregioncode']) }} as sk_territory
            , stateprovinceid
            , stateprovince_name
            , territoryid
            , countryregioncode
            , country_region_name
            , addressid
            , city

        from stateprovince
        left join countryregion on countryregion.countryregioncode = stateprovince.countryregioncode
        left join address on address.stateprovinceid = address.stateprovinceid
    )

select *
from dim_territory