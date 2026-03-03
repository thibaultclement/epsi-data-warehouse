-- Test métier : refused = requests - admitted
select *
from {{ ref('gold_fact_service_weekly') }}
where refused_count != (requests_count - admitted_count)
