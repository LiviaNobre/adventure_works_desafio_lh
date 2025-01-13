with 

entidade_negocio as (

    select
        cast(businessentityid as int) as id_entidade_negocio
        , cast(rowguid as int) as linha_guia

    from {{ source('sap_adw', 'businessentity') }}

)

select *
from entidade_negocio
