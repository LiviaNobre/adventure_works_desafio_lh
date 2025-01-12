with 

    motivo_venda as (

        select
            cast(salesreasonid as int) as id_razao_venda
            , name as nome_razao
            , reasontype as tipo_razao
            , cast(format_timestamp('%Y-%m-%m %H:%M:%S', cast(modifieddate as timestamp)) as timestamp) as data_modificacao

        from {{ source('sap_adw', 'salesreason') }}

    )

select *
from motivo_venda
