{{ config(materialized='table') }}

with weeks as (
    select distinct cast(yearweek as integer) as yearweek
    from {{ ref('gold_fact_service_weekly') }}
)

select
    yearweek,
    cast(yearweek / 100 as integer) as year,
    cast(yearweek % 100 as integer) as week_of_year
from weeks
