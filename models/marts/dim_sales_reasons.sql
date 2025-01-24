with

    salesreason as (
        select
            salesreasonid,
            reason_name,
            reasontype
        from {{ ref("stg_sap_adw__salesreason") }}
    )

    , salesorderheadersalesreason as (
        select
            salesorderid,
            salesreasonid
        from {{ ref("stg_sap_adw__salesorderheadersalesreason") }}
    )

    ''', salesorderheader as (
        select
            salesorderid
        from {{ ref("stg_sap_adw__salesorderheader") }}
    )'''

    , sales_reasons as (
        select
            {{ dbt_utils.generate_surrogate_key(['salesorderheadersalesreason.salesorderid', 'salesreason.salesreasonid']) }} as sk_sales_reason
            , salesorderheadersalesreason.salesorderid
            , coalesce(string_agg(salesreason.reason_name, ', '), 'Not Informed') as reason_names 
            , coalesce(string_agg(salesreason.reasontype, ', '), 'Not Informed') as reason_types
        from salesorderheadersalesreason
        --left join salesorderheadersalesreason on salesorderheadersalesreason.salesorderid = salesorderheader.salesorderid
        left join salesreason on salesreason.salesreasonid = salesorderheadersalesreason.salesreasonid
        group by salesorderheadersalesreason.salesorderid
    )

select *
from sales_reasons
