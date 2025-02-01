{{ config(materialized='table') }}

with combined as (

    select * from {{ ref('e_online_order')}}
    union all
    select * from {{ ref('e_instore_order')}}

)

select *
from combined