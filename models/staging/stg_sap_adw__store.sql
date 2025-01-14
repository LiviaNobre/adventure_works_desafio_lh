with 

    store as (

        select
            cast(businessentityid as int) as businessentityid 
            , cast(name as string) as store_name   
            , cast(salespersonid as int) as salespersonid
              
        from {{ source('sap_adw', 'store') }}

    )

select *
from loja
