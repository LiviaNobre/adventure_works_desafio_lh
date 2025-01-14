with 

    product as (

        select
            cast(productid as int) as productid
            , cast(productsubcategoryid as int) as productsubcategoryid
            , cast(productmodelid as int) as productmodelid
            , cast(name as string) as product_name
            , cast(makeflag as boolean) as makeflag
            , cast(color as string) as color
            , cast(safetystocklevel as int) as safetystocklevel
            , cast(reorderpoint as int) as reorderpoint
            , cast(standardcost as float64) as standardcost
            , cast(listprice as float64) as listprice
            , cast(size as string) as size
            , cast(weight as float64) as weight
            , cast(daystomanufacture as int) as daystomanufacture
            , cast(sellstartdate as timestamp) as sellstartdate
            , cast(sellenddate as timestamp) as sellenddate
            , cast(class as string) as class
            , cast(style as string) as style

        from {{ source('sap_adw', 'product') }}

    )

select *
from product
