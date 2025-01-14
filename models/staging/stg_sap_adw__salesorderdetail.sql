with

    salesorderdetail as (

        select
            cast(salesorderid as int) as salesorderid
            , cast(salesorderdetailid as int) as salesorderdetailid
            , cast(carriertrackingnumber as string) as carriertrackingnumber
            , cast(orderqty as int) as orderqty
            , cast(productid as int) as productid
            , cast(specialofferid as int) as specialofferid
            , cast(unitprice as float64) as unitprice
            , cast(unitpricediscount as float64) as unitpricediscount

        from {{ source('sap_adw', 'salesorderdetail') }}

    )

select * 
from salesorderdetail
