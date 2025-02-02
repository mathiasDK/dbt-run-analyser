{{ config(materialized='table') }}

with combined_orders as (

    select 
        order_id,
        'online' as store_id,
        customer_id,
        date as order_date
    from {{ ref('e_online_order')}}
    
    union all
    
    select 
        order_id,
        store_id,
        customer_id,
        date as order_date
    from {{ ref('e_instore_order')}}

)

select *
from combined_orders