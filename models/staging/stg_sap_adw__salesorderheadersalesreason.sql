with

    salesorderheadersalesreason as (

        select
            cast(salesorderid as int) as salesorderid
            , cast(salesreasonid as int) as salesreasonid

        from {{ source('sap_adw', 'salesorderheadersalesreason') }}

    )

select *
from salesorderheadersalesreason
