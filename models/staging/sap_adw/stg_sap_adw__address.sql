with

    endereco as (

        select
            cast(addressid as int) as id_endereco
            , cast(addressline1 as int) as linha_endereco_1
            , cast(addressline2 as int) as linha_endereco_2
            , city as cidade
            , cast(stateprovinceid as int) as id_estado_provincia
            , postalcode as codigo_postal
            , spatiallocation as localizacao_espacial
            , cast(format_timestamp('%Y-%m-%m %H:%M:%S', cast(modifieddate as timestamp)) as timestamp) as data_modificacao

        from {{ source('sap_adw', 'address') }}

    )

select *
from endereco
