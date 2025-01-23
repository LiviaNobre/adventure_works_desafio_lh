with
    stateprovince as (
        select
            stateprovinceid
            , countryregioncode
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
            , stateprovinceid
            , city
        from {{ ref("stg_sap_adw__address") }}
    )

    , dim_territory as (
        select
            {{ dbt_utils.generate_surrogate_key(['stateprovince.stateprovinceid', 'address.addressid']) }} as sk_territory
            , stateprovince.stateprovinceid as stateprovince_state_id
            , stateprovince.stateprovince_name
            , stateprovince.territoryid
            , stateprovince.countryregioncode
            , countryregion.country_region_name
            , address.addressid
            , address.city
        from stateprovince
        left join countryregion on countryregion.countryregioncode = stateprovince.countryregioncode
        left join address on address.stateprovinceid = stateprovince.stateprovinceid
    )
select *
from dim_territory
