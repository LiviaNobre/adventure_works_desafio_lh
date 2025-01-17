with

    address as (

        select
            cast(addressid as int) as addressid
            , cast(addressline1 as string) as addressline1
            , cast(addressline2 as string) as addressline2 
            , cast(city as string) city
            , cast(stateprovinceid as int) as stateprovinceid

        from {{ source('sap_adw', 'address') }}

    )

select *
from address
