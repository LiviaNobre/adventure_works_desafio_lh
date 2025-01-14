with

    salesorderheader as (
        select
            salesorderid
            , orderdate
            , duedate
            , shipdate
            , status
            , onlineorderflag
            , customerid
            , territoryid
            , salespersonid
            , creditcardapprovalcode
            , subtotal
            , taxamt
            , freight
            , totaldue
        from {{ ref("stg_sap_adw__salesorderheader") }}
    )

    , salesorderdetail as (
        select
            salesorderid
            , salesorderdetailid
            , orderqty
            , productid
            , specialofferid
            , unitprice
            , unitpricediscount
        from {{ ref("stg_sap_adw__salesorderdetail") }}
    )

    , fct_sales as (
        select
            {{ dbt_utils.generate_surrogate_key(['salesorderheader.salesorderid', 'salesorderdetail.salesorderdetailid']) }} as sk_sales
            --, salesorderid
            , orderdate
            , duedate
            , shipdate
            , status
            , onlineorderflag
            , customerid
            , territoryid
            , salespersonid
            , creditcardapprovalcode
            , subtotal
            , taxamt
            , freight
            , totaldue
            , salesorderdetailid
            , orderqty
            , productid
            , specialofferid
            , unitprice
            , unitpricediscount

        from salesorderheader
        left join salesorderdetail on salesorderdetail.salesorderid = salesorderheader.salesorderid
    )

select *
from fct_sales