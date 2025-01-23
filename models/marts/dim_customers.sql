with

    customer as (
        select
            customerid
            , personid
            , storeid
            , territoryid
        from {{ ref("stg_sap_adw__customer") }}
    )

    , person as (
        select
            businessentityid
            , firstname
            , lastname
            , persontype
            , case
                when emailpromotion = "0" then "Não recebe e-mail promocional"
                when emailpromotion = "1" then "Recebe e-mail promocional"
                when emailpromotion = "2" then "Recebe e-mail promocional e engaja"
                when emailpromotion is null then "Não se aplica"
            end as emailpromotion
        from {{ ref("stg_sap_adw__person") }}
    )

    , store as(
        select
            businessentityid
            , store_name
            , salespersonid
        from {{ ref("stg_sap_adw__store") }}
    )

    , dim_customers as (
        select
            {{ dbt_utils.generate_surrogate_key(['customer.customerid', 'customer.personid']) }} as sk_customer
            , customer.customerid
            , customer.personid
            , customer.storeid
            , customer.territoryid
            , person.businessentityid
            , person.firstname
            , person.lastname
            , person.persontype
            , store.store_name
            , store.salespersonid
            , person.emailpromotion

        from customer
        left join person on person.businessentityid = customer.personid
        left join store on store.businessentityid = customer.storeid

    )

select *
from dim_customers