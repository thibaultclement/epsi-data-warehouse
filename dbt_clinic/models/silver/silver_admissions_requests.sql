with ranked as (
    select *,
        row_number() over (
            partition by request_id
            order by batch_id desc
        ) as rn
    from bronze_admissions_requests
),

src as (
    select * from ranked where rn = 1
),

clean as (
  select
    trim(request_id) as request_id,
    cast(nullif(yearweek,'') as integer) as yearweek,
    lower(trim(service)) as service,
    cast(nullif(accepted,'') as integer) as accepted,
    nullif(lower(trim(reason)), '') as reason,
    cast(batch_id as integer) as batch_id,
    ingestion_ts,
    source_file_name
  from src
)

select *
from clean