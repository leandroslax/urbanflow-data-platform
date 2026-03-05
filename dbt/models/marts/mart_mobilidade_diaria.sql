select
  city,
  date_trunc('day', start_ts) as day_ts,
  count(*) as total_viagens
from {{ ref('stg_viagens') }}
group by 1,2
