{{ config(materialized='table') }}

with
-- 1) Agrégation des demandes/admissions/refus (source la plus “directe”)
req as (
    select
        lower(trim(service)) as service,
        cast(yearweek as integer) as yearweek,
        count(*) as requests_count,
        sum(case when cast(accepted as integer) = 1 then 1 else 0 end) as admitted_count,
        sum(case when cast(accepted as integer) = 0 then 1 else 0 end) as refused_count
    from {{ ref('silver_admissions_requests') }}
    group by 1, 2
),

-- 2) Agrégation patients (présents, LOS, satisfaction)
pat as (
    select
        lower(trim(service)) as service,
        cast(arrival_yearweek as integer) as yearweek,

        count(distinct patient_id) as patients_present_count,
        avg(cast(los as real)) as avg_los,
        avg(cast(satisfaction as real)) as avg_satisfaction
    from {{ ref('silver_patients') }}
    where arrival_yearweek is not null
    group by 1, 2
),

-- 3) Agrégation présence staff
-- Cas A (simple) : 1 ligne dans schedule = présent
-- Cas B : colonne present (0/1). On gère les deux : si present est null → on compte la ligne.
staff as (
    select
        -- si staff a un service : on l'utilise ; sinon on garde service = null (et on ne joint pas au service)
        lower(trim(coalesce(st.service, 'unknown'))) as service,
        cast(sc.yearweek as integer) as yearweek,
        sum(
            case
                when sc.present is null then 1
                when cast(sc.present as integer) = 1 then 1
                else 0
            end
        ) as staff_present_count
    from {{ ref('silver_staff_schedule') }} sc
    left join {{ ref('silver_staff') }} st
      on st.staff_name = sc.staff_name
    group by 1, 2
),

-- 4) Base (union des clés) pour ne pas perdre de combinaisons (service, yearweek)
keys as (
    select service, yearweek from req
    union
    select service, yearweek from pat
    union
    select service, yearweek from staff
)

select
    -- PK technique au grain (service, week)
    replace(lower(trim(k.service)), ' ', '_') || '_' || cast(k.yearweek as text) as service_week_id,

    -- FK dimensions
    replace(lower(trim(k.service)), ' ', '_') as service_id,
    cast(k.yearweek as integer) as yearweek,

    -- mesures
    coalesce(r.requests_count, 0) as requests_count,
    coalesce(r.admitted_count, 0) as admitted_count,
    coalesce(r.refused_count, 0) as refused_count,

    case
        when coalesce(r.requests_count, 0) = 0 then 0.0
        else 1.0 * coalesce(r.refused_count, 0) / r.requests_count
    end as refusal_rate,

    coalesce(p.patients_present_count, 0) as patients_present_count,
    p.avg_los as avg_los,
    p.avg_satisfaction as avg_satisfaction,

    case when s.service = 'unknown' then null else s.staff_present_count end as staff_present_count

from keys k
left join req r
  on r.service = k.service and r.yearweek = k.yearweek
left join pat p
  on p.service = k.service and p.yearweek = k.yearweek
left join staff s
  on s.service = k.service and s.yearweek = k.yearweek
