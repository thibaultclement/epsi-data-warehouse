with ranked as (
    select *,
        row_number() over (
            partition by patient_id
            order by batch_id desc
        ) as rn
    from bronze_patients
),

src as (
    select *
    from ranked
    where rn = 1
),

clean as (
    select
        patient_id,
        trim(name) as name,

        cast(nullif(age, '') as integer) as age,

        cast(nullif(arrival_yearweek, '') as integer) as arrival_yearweek,
        cast(nullif(departure_yearweek, '') as integer) as departure_yearweek,

        case
            when arrival_date = '' then null
            else substr(arrival_date, 7, 4) || '-' || substr(arrival_date, 4, 2) || '-' || substr(arrival_date, 1, 2)
        end as arrival_date,

        case
            when departure_date = '' then null
            else substr(departure_date, 7, 4) || '-' || substr(departure_date, 4, 2) || '-' || substr(departure_date, 1, 2)
        end as departure_date,

        lower(trim(service)) as service,

        cast(replace(nullif(satisfaction,''), ',', '.') as real) as satisfaction,

        cast(nullif(los, '') as integer) as los,

        nullif(weeks_active, '') as weeks_active,
        nullif(request_id, '') as request_id,

        cast(batch_id as integer) as batch_id,
        ingestion_ts,
        source_file_name

    from src
)

select *
from clean