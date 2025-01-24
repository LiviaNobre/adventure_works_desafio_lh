with

productcategory as (

    select
        cast(productcategoryid as int) as productcategoryid
        , cast(name as string) as category_name

    from {{ source('sap_adw', 'productcategory') }}

)

select *
from productcategory
