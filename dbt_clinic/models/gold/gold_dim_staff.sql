{{ config(materialized='table') }}

select
    staff_id,
    staff_name,
    role,
    service
from {{ ref('silver_staff') }}
