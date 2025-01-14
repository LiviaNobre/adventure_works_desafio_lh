with
    product as (
        select
            productid
            , product_name
            , color
            , productsubcategoryid
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

    , subcategory as (
        select
            subcategory_name
        from {{ ref("stg_sap_adw__productsubcategory") }}
    )

    , category as (
        select
            productcategoryid
            , category_name
        from {{ ref("stg_sap_adw__productcategory") }}
    )

    , dim_product as (
        select
            {{ dbt_utils.generate_surrogate_key(['productid', 'product_name']) }} as sk_product
            , productid
            , product_name
            , color
            , productsubcategoryid
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
            , productcategoryid
            , category_name

        from product
        left join subcategoryid on subcategoryid.productsubcategoryid = product.productsubcategoryid
        left join category on category.productcategoryid = subcategory.productcategoryid
    )

select *
from dim_product