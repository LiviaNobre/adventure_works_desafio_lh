with

    razao_pedido_venda as (

        select
            cast(salesorderid as int) as id_vendas
            , cast(salesreasonid as int) as id_razao_venda
            , cast(format_timestamp('%Y-%m-%m %H:%M:%S', cast(modifieddate as timestamp)) as timestamp) as data_modificacao

        from {{ source('sap_adw', 'salesorderheadersalesreason') }}

    )

select *
from razao_pedido_venda
