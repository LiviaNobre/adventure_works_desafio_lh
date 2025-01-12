with

    razao_venda as (
        select *
        from {{ ref("stg_sap_adw__salesreason") }}
    )

    , razao_pedido_venda as (
        select *
        from {{ ref("stg_sap_adw__salesorderheadersalesreason") }}
    )

    , join_tabelas as (
        select
            razao_venda.id_razao_venda
            , razao_venda.nome_razao
            , razao_venda.tipo_razao
            , razao_pedido_venda.id_vendas
           
        from razao_pedido_venda
        left join razao_venda on razao_venda.id_razao_venda = razao_pedido_venda.id_razao_venda
    ),

    dim_razao_venda as (
        select
            {{ dbt_utils.generate_surrogate_key(['id_razao_venda', 'id_vendas']) }} as sk_razao_venda
            , id_razao_venda
            , id_vendas
            , nome_razao
            , tipo_razao
        from join_tabelas
    )

select *
from dim_razao_venda