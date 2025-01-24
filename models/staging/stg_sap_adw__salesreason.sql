with 

    salesreason as (

        select
            cast(salesreasonid as int) as salesreasonid
            , cast(name as string) as reason_name
            , cast(reasontype as string) as reasontype

        from {{ source('sap_adw', 'salesreason') }}

    )

select *
from salesreason
