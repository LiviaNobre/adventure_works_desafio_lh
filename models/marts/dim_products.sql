with
    product as (
        select
            productid
            , productsubcategoryid
            , product_name
            , color
            , safetystocklevel
            , reorderpoint
            , standardcost
            , listprice
            , size
            , weight
            , daystomanufacture
            , sellstartdate
            , sellenddate
        from {{ ref("stg_sap_adw__product") }}
    )

    , category as (
        select
            productcategoryid
            , category_name
        from {{ ref("stg_sap_adw__productcategory") }}
    )

    , subcategory as (
        select
            productcategoryid
            , productsubcategoryid
            , subcategory_name
        from {{ ref("stg_sap_adw__productsubcategory") }}
    )

    , dim_products as (
        select
            {{ dbt_utils.generate_surrogate_key(['productid', 'product_name']) }} as sk_product
            , productid
            , product_name
            , color
            , safetystocklevel
            , reorderpoint
            , standardcost
            , listprice
            , size
            , weight
            , daystomanufacture
            , sellstartdate
            , sellenddate
            , subcategory_name
            , category_name

        from product
        left join subcategory on subcategory.productsubcategoryid = product.productsubcategoryid
        left join category on category.productcategoryid = subcategory.productcategoryid
    )

select *
from dim_products