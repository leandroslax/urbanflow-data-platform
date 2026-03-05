select
  city,
  date_trunc('hour', event_ts) as hour_ts,
  avg(congestion_index) as avg_congestion_index
from {{ ref('stg_trafego') }}
group by 1,2
