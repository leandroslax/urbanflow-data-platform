select
  city,
  region,
  date_trunc('day', event_ts) as day_ts,
  count(*) as total_incidentes
from {{ ref('stg_incidentes') }}
group by 1,2,3
