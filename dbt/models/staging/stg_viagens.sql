select
  trip_id,
  city,
  region,
  start_ts,
  end_ts,
  driver_id,
  passenger_id,
  event_ts
from {{ source('silver','viagens') }}
