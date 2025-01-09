with

    detalhe_pedido_venda as (

        select
            cast(salesorderid as int) as id_pedido_venda
            , cast(salesorderdetailid as int) as id_detalhe_pedido_venda
            , carriertrackingnumber as numero_rastreamento_transportadora
            , cast(orderqty as int) as quantidade_pedido
            , cast(productid as int) as id_produto
            , cast(specialofferid as int) as id_oferta_especial
            , cast(unitprice as float64) as preco_unitario
            , cast(unitpricediscount as float64) as desconto_preco_unitario
            , cast(format_timestamp('%Y-%m-%m %H:%M:%S', cast(modifieddate as timestamp)) as timestamp) as data_modificacao
        from {{ source('sap_adw', 'salesorderdetail') }}

    )

select * 
from detalhe_pedido_venda
