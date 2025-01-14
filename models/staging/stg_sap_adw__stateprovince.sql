with 

    stateprovince as (

        select
            cast(stateprovinceid as int) as stateprovinceid
            , cast(stateprovincecode as string) as stateprovincecode
            , cast(countryregioncode as string) as countryregioncode
            , cast(isonlystateprovinceflag as boolean) as isonlystateprovinceflag
            , cast(name as string) as stateprovince_name
            , cast(territoryid as int) as territoryid
            
        from {{ source('sap_adw', 'stateprovince') }}

    )

select *
from stateprovince
