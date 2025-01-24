with
    person as (
        select *
        from {{ ref("stg_sap_adw__person") }}
    )
    , fct_sales as (
        select *
        from {{ ref("fct_sales") }}
    )
    , region as (
        select *
        from {{ ref("dim_territories") }}
    )
select 
    fct_sales.salespersonid
    , region.stateprovince_name
    , count(distinct fct_sales.salesorderid) as total_orders
    , sum(fct_sales.orderqty) as total__products_qty -- total de produtos comprados
    , sum(fct_sales.totaldue) as total_sales_value -- receita total
    , sum(fct_sales.subtotal) as total_subtotal
    , avg(fct_sales.taxamt) as total_tax
    , avg(fct_sales.freight) as total_freight
    , avg(fct_sales.totaldue) as average_order_value
    , count(distinct fct_sales.salesorderid) as total_qty_sales
    , count(distinct fct_sales.salespersonid) as total_salespersons
from fct_sales
left join person on person.businessentityid = fct_sales.salespersonid
left join region on region.addressid = fct_sales.shiptoaddressid
group by salespersonid, stateprovince_name