with 

    produto as (

        select
            cast(productid as int) as id_produto
            , name as nome
            , productnumber as numero_produto
            , makeflag as indicador_producao
            , finishedgoodsflag as indicador_produto_finalizado
            , color as cor
            , cast(safetystocklevel as int) as nivel_estoque_seguranca
            , cast(reorderpoint as int) as ponto_reabastecimento
            , cast(standardcost as float64) as custo_padrao
            , cast(listprice as float64) as lista_preco
            , size as tamanho
            , sizeunitmeasurecode as codigo_unidade_medida_tamanho
            , weightunitmeasurecode as codigo_unidade_medida_peso
            , cast(weight as float64) as peso
            , cast(daystomanufacture as int) as dias_fabricacao
            , productline as linha_produto
            , class as classe
            , style as estilo
            , cast(productsubcategoryid as int) as id_subcategoria_produto
            , cast(productmodelid as int) as id_modelo_produto
            , parse_timestamp('%Y-%m-%d %H:%M:%S.%f', cast(sellstartdate as string)) as data_inicio_venda
            , parse_timestamp('%Y-%m-%d %H:%M:%S.%f', cast(sellenddate as string)) as data_fim_venda
            , parse_timestamp('%Y-%m-%d %H:%M:%S.%f', cast(discontinueddate as string)) as data_descontinuacao
            , rowguid as linha_guia
            , cast(format_timestamp('%Y-%m-%d %H:%M:%S', cast(modifieddate as timestamp)) as timestamp) as data_modificacao

        from {{ source('sap_adw', 'product') }}

    )

select *
from produto
