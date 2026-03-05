select
  city,
  avg(datediff('second', start_ts, end_ts)) as avg_trip_duration_seconds
from {{ ref('stg_viagens') }}
where start_ts is not null and end_ts is not null
group by 1
