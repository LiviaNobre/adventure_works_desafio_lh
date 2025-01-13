with 

    produto as (

        select
            cast(productid as int) as id_produto
            , cast(productsubcategoryid as int) as id_subcategoria_produto
            , cast(productmodelid as int) as id_modelo_produto
            , name as nome_produto
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
            , cast(sellstartdate as timestamp) as data_inicio_venda
            , cast(sellenddate as timestamp) as data_fim_venda
            , productline as linha_produto
            , class as classe
            , style as estilo
            , cast(format_timestamp('%Y-%m-%d %H:%M:%S', cast(modifieddate as timestamp)) as timestamp) as data_modificacao

        from {{ source('sap_adw', 'product') }}

    )

select *
from produto
