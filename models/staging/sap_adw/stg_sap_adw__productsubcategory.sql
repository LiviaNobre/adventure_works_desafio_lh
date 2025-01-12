with

subcategoria_categoria as (

    select
        cast(productsubcategoryid as int) as id_subcategoria_produto
        , cast(productcategoryid as int) as id_categoria_produto
        , cast(name as string) as nome_subcategoria

    from {{ source('sap_adw', 'productsubcategory') }}

)

select *
from subcategoria_categoria
