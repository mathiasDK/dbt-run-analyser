{{ config(materialized='table') }}

select *
from {{ ref('stg_order')}}
left join {{ ref('e_order_item')}} using(order_id)