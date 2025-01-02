with 

    endereco as (

        select
            cast(addressid as int) as endereco,
            addressline1,
            --addressline2,
            city,
            stateprovinceid,
            postalcode,
            spatiallocation,
            rowguid,
            modifieddate
        from {{ source('sap_adw', 'address') }}

    )

select * from endereco
