with 

source as (

    select * from {{ source('sap_adw', 'department') }}

),

renamed as (

    select
        departmentid,
        name,
        groupname,
        modifieddate

    from source

)

select * from renamed
