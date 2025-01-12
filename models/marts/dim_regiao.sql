with

    estado_provincia as (
        select *
        from {{ ref("stg_sap_adw__stateprovince") }}
    )

    , pais_regiao as (
        select *
        from {{ ref("stg_sap_adw__countryregion") }}
    )

    , endereco as (
        select *
        from {{ ref("stg_sap_adw__address")}}
    )

    , join_tabelas as (
        select
            estado_provincia.id_estado_provincia
            , estado_provincia.nome_estado_provincia
            , estado_provincia.id_territorio
            , pais_regiao.codigo_pais_regiao
            , pais_regiao.nome_pais_regiao
            , endereco.id_endereco
            , endereco.cidade
           
        from estado_provincia
        left join pais_regiao on pais_regiao.codigo_pais_regiao = estado_provincia.codigo_pais_regiao
        left join endereco on endereco.id_estado_provincia = estado_provincia.id_estado_provincia
    ),

    dim_regiao as (
        select
            {{ dbt_utils.generate_surrogate_key(['id_estado_provincia', 'codigo_pais_regiao']) }} as sk_regiao
            , id_estado_provincia
            , nome_estado_provincia
            , id_territorio
            , codigo_pais_regiao
            , nome_pais_regiao
            , id_endereco
            , cidade

        from join_tabelas
    )

select *
from dim_regiao