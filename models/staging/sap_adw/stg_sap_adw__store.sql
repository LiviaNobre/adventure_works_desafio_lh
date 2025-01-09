with 

    loja as (

        select
            cast(businessentityid as int) as id_entidade_negocio 
            , name as nome   
            , cast(salespersonid as int) as id_vendedor  
            , demographics as demografia 
            , rowguid as linha_guia
            , cast(format_timestamp('%Y-%m-%m %H:%M:%S', cast(modifieddate as timestamp)) as timestamp) as data_modificacao  

        from {{ source('sap_adw', 'store') }}

    )

select *
from loja
