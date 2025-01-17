with

    credit_cards as (
        select
            creditcardid
            , cardtype
            , expmonth
            , expyear
        from {{ ref("stg_sap_adw__creditcard") }}
    )

        , dim_credit_cards as (
        select
            {{ dbt_utils.generate_surrogate_key(['creditcardid', 'cardtype']) }} as sk_creditcard
            , *
        from credit_cards
    )
    
select *
from dim_credit_cards