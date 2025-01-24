with
    salesorderheader as (
        select
            salesorderid
            , date(orderdate) as orderdate
            , duedate
            , shipdate
            , case
                when status = 1 then "In process"
                when status = 2 then "Approved"
                when status = 3 then "Backordered"
                when status = 4 then "Rejected"
                when status = 5 then "Shipped"
                when status = 6 then "Cancelled"
            end as status
            , case
                when onlineorderflag = True then "Online"
                else "In person"
            end as onlineorderflag
            , customerid
            , territoryid
            , shiptoaddressid
            , creditcardid
            , subtotal
            , taxamt
            , freight
            , totaldue
            , creditcardapprovalcode
        from {{ ref('stg_sap_adw__salesorderheader') }}
    )
    
    , salesorderdetail as (
        select *
        from {{ ref("stg_sap_adw__salesorderdetail") }}
    )

    , dim_credit_cards as (
        select
            sk_creditcard
            , creditcardid
        from {{ ref('dim_credit_cards') }}
    )

    , dim_customers as (
        select
            sk_customer
            , customerid
            , salespersonid
        from {{ ref('dim_customers') }}
    )

    , dim_territories as (
        select
            sk_territory
            , territoryid
            , addressid
        from {{ ref('dim_territories') }}
    )

    , dim_sales_reasons as (
        select
            sk_sales_reason
            , salesorderid
            , reason_names
        from {{ ref('dim_sales_reasons') }}
    )

    , dim_products as (
        select
            sk_product
            , productid
        from {{ ref('dim_products') }}
    )

    , dim_dates as (
        select
            sk_date
            , data_dia
        from {{ ref('dim_dates') }}
    )

    , fct_sales as (
        select
            {{ dbt_utils.generate_surrogate_key(['salesorderdetail.salesorderdetailid']) }} as sk_sales
            , salesorderheader.salesorderid
            , salesorderdetail.salesorderdetailid
            , dim_products.productid
            , dim_products.sk_product as fk_product
            , salesorderheader.orderdate
            , salesorderheader.duedate
            , salesorderheader.shipdate
            , salesorderheader.status
            , salesorderheader.onlineorderflag
            , salesorderheader.customerid
            , salesorderheader.territoryid
            , salesorderheader.shiptoaddressid
            --, salesorderheader.salespersonid
            , salesorderheader.creditcardapprovalcode
            , salesorderheader.subtotal
            , salesorderheader.taxamt
            , salesorderheader.freight
            , salesorderheader.totaldue
            , salesorderheader.creditcardid
            , salesorderdetail.orderqty
            , salesorderdetail.specialofferid
            , salesorderdetail.unitprice
            , salesorderdetail.unitpricediscount
            , (salesorderdetail.orderqty * salesorderdetail.unitprice) as total --bruto
            , ((1 - salesorderdetail.unitpricediscount) * salesorderdetail.unitprice * salesorderdetail.orderqty) as product_value --liquido
            , coalesce(dim_customers.salespersonid, 0) as salespersonid
            , dim_credit_cards.sk_creditcard as fk_creditcard
            , dim_sales_reasons.sk_sales_reason as fk_sales_reason
            , dim_territories.sk_territory as fk_territory
            , dim_dates.sk_date as fk_date
            , dim_customers.sk_customer as fk_customer
        from salesorderheader
        left join dim_customers on dim_customers.customerid = salesorderheader.customerid
        left join salesorderdetail on salesorderdetail.salesorderid = salesorderheader.salesorderid
        left join dim_products on dim_products.productid = salesorderdetail.productid
        left join dim_sales_reasons on dim_sales_reasons.salesorderid = salesorderheader.salesorderid
        left join dim_credit_cards on dim_credit_cards.creditcardid = salesorderheader.creditcardid
        left join dim_territories on dim_territories.addressid = salesorderheader.shiptoaddressid
        --left join dim_dates on cast(dim_dates.data_dia as date) = cast(salesorderheader.orderdate as date)
        left join dim_dates on dim_dates.data_dia = salesorderheader.orderdate
    )

select *
from fct_sales