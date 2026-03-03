-- silver_staff.sql

with ranked as (
    select *,
        row_number() over (
            partition by staff_name, role
            order by batch_id desc
        ) as rn
    from bronze_staff
),

src as (
  select * from ranked where rn = 1
),

clean as (
  select
    trim(staff_name) as staff_name,
    lower(trim(role)) as role,
    lower(trim(service)) as service,

    -- création surrogate key
    replace(lower(trim(staff_name)), ' ', '_')
    || '_'
    || lower(trim(role)) as staff_id,

    cast(batch_id as integer) as batch_id,
    ingestion_ts,
    source_file_name
  from src
)

select * from clean