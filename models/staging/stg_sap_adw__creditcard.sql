with 

    creditcard as (

        select
            cast(creditcardid as int) as creditcardid
            , cast(cardtype as string) as cardtype 
            , cast(cardnumber as int) as cardnumber
            , cast(expmonth as int) as expmonth
            , cast(expyear as int) as expyear

        from {{ source('sap_adw', 'creditcard') }}

    )

select *
from creditcard
