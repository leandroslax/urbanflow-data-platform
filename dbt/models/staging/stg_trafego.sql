select
  traffic_id,
  city,
  region,
  congestion_index,
  travel_time_seconds,
  free_flow_time_seconds,
  event_ts
from {{ source('silver','trafego') }}
