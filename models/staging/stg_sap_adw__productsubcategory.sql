with

productsubcategory as (

    select
        cast(productsubcategoryid as int) as productsubcategoryid
        , cast(productcategoryid as int) as productcategoryid
        , cast(name as string) as subcategory_name

    from {{ source('sap_adw', 'productsubcategory') }}

)

select *
from productsubcategory
