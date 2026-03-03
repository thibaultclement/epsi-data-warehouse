with ranked as (

    select *,
        row_number() over (
            partition by yearweek, staff_name
            order by batch_id desc
        ) as rn
    from bronze_staff_schedule

),

src as (

    select
        cast(nullif(yearweek,'') as integer) as yearweek,
        trim(staff_name) as staff_name,
        cast(nullif(present,'') as integer) as present,
        cast(batch_id as integer) as batch_id,
        ingestion_ts,
        source_file_name
    from ranked
    where rn = 1

),

staff_ref as (

    select distinct
        trim(staff_name) as staff_name,
        lower(trim(role)) as role
    from bronze_staff

)

select
    s.yearweek,
    s.staff_name,
    st.role,

    -- surrogate key déterministe
    replace(lower(trim(s.staff_name)), ' ', '_')
    || '_'
    || lower(trim(st.role)) as staff_id,

    s.present,
    s.batch_id,
    s.ingestion_ts,
    s.source_file_name

from src s
left join staff_ref st
    on st.staff_name = s.staff_name