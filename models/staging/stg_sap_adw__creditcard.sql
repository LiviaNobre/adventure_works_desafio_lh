with 

    cartao_credito as (

        select
            cast(creditcardid as int) as id_cartao_credito
            , cardtype as tipo_cartao 
            , cardnumber as numero_cartao
            , expmonth as mes_vencimento 
            , expyear as ano_vencimento
            , cast(format_timestamp('%Y-%m-%m %H:%M:%S', cast(modifieddate as timestamp)) as timestamp) as data_modificacao

        from {{ source('sap_adw', 'creditcard') }}

    )

select *
from cartao_credito
