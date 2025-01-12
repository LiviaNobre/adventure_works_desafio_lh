with
    produto as (
        select *
        from {{ ref("stg_sap_adw__product") }}
    )


    , subcategoria as (
        select *
        from {{ ref("stg_sap_adw__productsubcategory") }}
    )


    , categoria as (
        select *
        from {{ ref("stg_sap_adw__productcategory") }}
    )


    , join_tabelas as (
        select
            produto.id_produto
            , produto.nome_produto
            , produto.cor
            , produto.nivel_estoque_seguranca
            , produto.ponto_reabastecimento
            , produto.custo_padrao
            , produto.lista_preco
            , produto.tamanho
            , produto.peso
            , produto.dias_fabricacao
            , produto.id_subcategoria_produto
            , produto.data_fim_venda
            , produto.data_inicio_venda
            , subcategoria.id_categoria_produto
            , subcategoria.nome_subcategoria
            , categoria.nome_categoria
        from produto
        left join subcategoria on subcategoria.id_subcategoria_produto = produto.id_subcategoria_produto
        left join categoria on categoria.id_categoria_produto = subcategoria.id_categoria_produto
    )


    , dim_produto as (
        select
            {{ dbt_utils.generate_surrogate_key(['id_produto', 'nome_produto']) }} as sk_produto
            , id_produto
            , nome_produto
            , cor
            , nivel_estoque_seguranca
            , ponto_reabastecimento
            , custo_padrao
            , lista_preco
            , tamanho
            , peso
            , dias_fabricacao
            , id_subcategoria_produto
            , data_fim_venda
            , data_inicio_venda
            , id_categoria_produto
            , nome_subcategoria
            , nome_categoria
        from join_tabelas
    )


select *
from dim_produto