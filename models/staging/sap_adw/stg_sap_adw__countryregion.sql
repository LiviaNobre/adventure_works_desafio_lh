with 

    pais_regiao as (

        select
            countryregioncode as codigo_pais_regiao 
            , name as nome_pais_regiao
            , cast(format_timestamp('%Y-%m-%m %H:%M:%S', cast(modifieddate as timestamp)) as timestamp) as data_modificacao

        from {{ source('sap_adw', 'countryregion') }}

    )

select *
from pais_regiao
