with

    customer as (
        select
            customerid
            , personid
        from {{ ref("stg_sap_adw__customer") }}
    )

    , person as (
        select
            businessentityid
            , firstname
            , lastname
        from {{ ref("stg_sap_adw__person") }}
    )

    dim_customer as (
        select
            {{ dbt_utils.generate_surrogate_key(['customerid', 'personid']) }} as sk_customer
            , customerid
            , personid
            , businessentityid
            , firstname
            , lastname

        from customer
        left join person on person.businessentityid = customers.personid

    )

select *
from dim_customer