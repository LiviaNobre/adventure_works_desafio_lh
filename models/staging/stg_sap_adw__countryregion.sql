with 

    countryregion as (

        select
            cast(countryregioncode as string) as countryregioncode
            , cast(name as string) country_region_name

        from {{ source('sap_adw', 'countryregion') }}

    )

select *
from countryregion
