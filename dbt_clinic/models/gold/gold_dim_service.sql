{{ config(materialized='table') }}

with services as (
    select distinct lower(trim(service)) as service
    from {{ ref('silver_admissions_requests') }}
    where service is not null

    union

    select distinct lower(trim(service)) as service
    from {{ ref('silver_patients') }}
    where service is not null
)

select
    replace(service, ' ', '_') as service_id,
    service as service_name
from services
