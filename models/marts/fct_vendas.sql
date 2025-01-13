with

    pedido_venda as (
        select *
        from {{ ref("stg_sap_adw__salesorderheader") }}
    )

    , detalhe_pedido_venda as (
        select *
        from {{ ref("stg_sap_adw__salesorderdetail") }}
    )

    , join_tabelas as (
        select
            pedido_venda.id_pedido_venda
            , pedido_venda.data_pedido
            , pedido_venda.data_entrega
            , pedido_venda.data_envio
            , pedido_venda.indicador_pedido_online
            , pedido_venda.id_cliente
            , pedido_venda.id_territorio
            , pedido_venda.id_vendedor
            , pedido_venda.codigo_aprovacao_cartao_credito
            , pedido_venda.subtotal
            , pedido_venda.valor_imposto
            , pedido_venda.frete
            , pedido_venda.valor_devido
            , detalhe_pedido_venda.id_detalhe_pedido_venda
            , detalhe_pedido_venda.quantidade_pedido
            , detalhe_pedido_venda.id_produto
            , detalhe_pedido_venda.preco_unitario
            , detalhe_pedido_venda.desconto_preco_unitario
                       
        from pedido_venda
        left join detalhe_pedido_venda on pedido_venda.id_pedido_venda = detalhe_pedido_venda.id_pedido_venda

    ),

    fct_venda as (
        select
            {{ dbt_utils.generate_surrogate_key(['id_pedido_venda', 'id_detalhe_pedido_venda']) }} as sk_venda
            , id_pedido_venda
            , data_pedido
            , data_entrega
            , data_envio
            , indicador_pedido_online
            , id_cliente
            , id_territorio
            , id_vendedor
            , codigo_aprovacao_cartao_credito
            , subtotal
            , valor_imposto
            , frete
            , valor_devido
            , id_detalhe_pedido_venda
            , quantidade_pedido
            , id_produto
            , preco_unitario
            , desconto_preco_unitario
        from join_tabelas
    )

select *
from fct_venda