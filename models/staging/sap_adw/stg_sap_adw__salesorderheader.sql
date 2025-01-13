with

    pedido_venda as (

        select
            cast(salesorderid as int) as id_pedido_venda
            , revisionnumber as numero_revisao
            , cast(format_timestamp('%Y-%m-%m %H:%M:%S', cast(orderdate as timestamp)) as timestamp) as data_pedido
            , cast(format_timestamp('%Y-%m-%m %H:%M:%S', cast(duedate as timestamp)) as timestamp) as data_entrega
            , cast(format_timestamp('%Y-%m-%m %H:%M:%S', cast(shipdate as timestamp)) as timestamp) as data_envio
            , status
            , onlineorderflag as indicador_pedido_online
            , purchaseordernumber as numero_compra
            , accountnumber as numero_conta
            , cast(customerid as int) as id_cliente
            , cast(salespersonid as int) as id_vendedor
            , cast(territoryid as int) as id_territorio
            , cast(billtoaddressid as int) as id_endereco_cobranca 
            , cast(shiptoaddressid as int) as id_endereco_entrega
            , cast(shipmethodid as int) as id_metodo_envio
            , cast(creditcardid as int) as id_cartao_credito
            , creditcardapprovalcode as codigo_aprovacao_cartao_credito
            , cast(currencyrateid as int) as id_taxa_cambio
            , cast(subtotal as float64) as subtotal
            , cast(taxamt as float64) as valor_imposto
            , cast(freight as float64) as frete
            , cast(totaldue as float64) as valor_devido 
            , comment as comentario
            , cast(format_timestamp('%Y-%m-%m %H:%M:%S', cast(modifieddate as timestamp)) as timestamp) as data_modificacao

        from {{ source('sap_adw', 'salesorderheader') }}

    )

select *
from pedido_venda
