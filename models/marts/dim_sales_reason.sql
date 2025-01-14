with

    salesreason as (
        select
            salesreasonid
            , reason_name
            , reasontype
        from {{ ref("stg_sap_adw__salesreason") }}
    )

    , salesorderheadersalesreason as (
        select
            salesorderid
            , salesreasonid
        from {{ ref("stg_sap_adw__salesorderheadersalesreason") }}
    )

    , dim_sales_reason as (
        select
            {{ dbt_utils.generate_surrogate_key(['salesreason.salesreasonid', 'salesorderheadersalesreason.salesorderid']) }} as sk_sales_reason
            , reason_name
            , reasontype
            , salesorderid
        from salesreason
        left join salesorderheadersalesreason on salesorderheadersalesreason.salesreasonid = salesreason.salesreasonid
    )

select *
from dim_sales_reason