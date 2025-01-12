with

categoria_produto as (

    select
        cast(productcategoryid as int) as id_categoria_produto
        , cast(name as string) as nome_categoria

    from {{ source('sap_adw', 'productcategory') }}

)

select *
from categoria_produto
