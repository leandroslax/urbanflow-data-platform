select
  gps_id,
  trip_id,
  city,
  region,
  lat,
  lon,
  speed,
  event_ts
from {{ source('silver','gps') }}
