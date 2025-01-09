with 

    pessoa as (

        select
            cast(businessentityid as int) as id_entidade_negocio
            , title as titulo 
            , firstname as primeiro_nome
            , middlename as nome_meio
            , lastname as ultimo_nome
            , suffix as sufixo
            , emailpromotion as email_promocao
            , demographics as demografia 
            , cast(format_timestamp('%Y-%m-%m %H:%M:%S', cast(modifieddate as timestamp)) as timestamp) as data_modificacao


        from {{ source('sap_adw', 'person') }}

    )

select *
from pessoa
