with

    salesorderheader as (

        select
            cast(salesorderid as int) as salesorderid
            --, format_date('%Y-%m-%d', orderdate) as orderdate
            , date(orderdate) as orderdate
            , date(duedate) as duedate
            , date(shipdate) as shipdate
            --, cast(format_timestamp('%Y-%m-%m %H:%M:%S', cast(orderdate as timestamp)) as timestamp) as orderdate
            --, cast(format_timestamp('%Y-%m-%m %H:%M:%S', cast(duedate as timestamp)) as timestamp) as duedate
            --, cast(format_timestamp('%Y-%m-%m %H:%M:%S', cast(shipdate as timestamp)) as timestamp) as shipdate
            , cast(status as int) as status
            , cast(onlineorderflag as boolean) as onlineorderflag
            , cast(purchaseordernumber as string) as purchaseordernumber
            , cast(accountnumber as string) as accountnumber
            , cast(customerid as int) as customerid
            , cast(salespersonid as int) as salespersonid
            , cast(territoryid as int) as territoryid
            , cast(billtoaddressid as int) as billtoaddressid 
            , cast(shiptoaddressid as int) as shiptoaddressid
            , cast(shipmethodid as int) as shipmethodid
            , cast(creditcardid as int) as creditcardid
            , cast(creditcardapprovalcode as string) as creditcardapprovalcode
            , cast(currencyrateid as int) as currencyrateid
            , cast(subtotal as float64) as subtotal
            , cast(taxamt as float64) as taxamt
            , cast(freight as float64) as freight
            , cast(totaldue as float64) as totaldue

        from {{ source('sap_adw', 'salesorderheader') }}

    )

select *
from salesorderheader
