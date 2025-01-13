with

    cliente as (
        select *
        from {{ ref("stg_sap_adw__customer") }}
    )

    , pessoa as (
        select *
        from {{ ref("stg_sap_adw__person") }}
    )

    , endereco as (
        select *
        from {{ ref("stg_sap_adw__address") }}
    )

    , estado_provincia as (
        select *
        from {{ ref("stg_sap_adw__stateprovince") }}
    )

    , pais_regiao as (
        select *
        from {{ ref("stg_sap_adw__countryregion") }}
    )

    , join_tabelas as (
        select
            pessoa.id_entidade_negocio
            , pessoa.primeiro_nome
            , pessoa.ultimo_nome
            , cliente.id_cliente
            , cliente.id_pessoa
            , cliente.id_territorio
            , estado_provincia.id_estado_provincia
            , estado_provincia.nome_estado_provincia
            , endereco.cidade
            , pais_regiao.codigo_pais_regiao
            , pais_regiao.nome_pais_regiao
           
        from cliente
        left join pessoa on cliente.id_pessoa = pessoa.id_entidade_negocio
        left join estado_provincia on cliente.id_territorio = estado_provincia.id_territorio
        left join pais_regiao on estado_provincia.codigo_pais_regiao = pais_regiao.codigo_pais_regiao
        left join endereco on estado_provincia.id_estado_provincia = endereco.id_estado_provincia
    ),

    dim_cliente as (
        select
            {{ dbt_utils.generate_surrogate_key(['id_cliente', 'id_pessoa']) }} as sk_cliente
            , id_entidade_negocio
            , primeiro_nome
            , ultimo_nome
            , id_cliente
            , id_pessoa
            , id_territorio
            , id_estado_provincia
            , nome_estado_provincia
            , cidade
            , codigo_pais_regiao
            , nome_pais_regiao
        from join_tabelas
    )

select *
from dim_cliente