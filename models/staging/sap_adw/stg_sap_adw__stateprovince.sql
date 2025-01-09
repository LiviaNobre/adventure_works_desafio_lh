with 

    estado_provincia as (

        select
            cast(stateprovinceid as int) as id_estado_provincia
            , stateprovincecode as codigo_provincia_estado
            , countryregioncode as codigo_pais_regiao
            , isonlystateprovinceflag as indicador_unico_estado_provincia
            , name as nome
            , cast(territoryid as int) as id_territorio 
            , rowguid as linha_guia
            , cast(format_timestamp('%Y-%m-%m %H:%M:%S', cast(modifieddate as timestamp)) as timestamp) as data_modificacao

        from {{ source('sap_adw', 'stateprovince') }}

    )

select *
from estado_provincia
