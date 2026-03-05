select
  weather_id,
  city,
  region,
  temperature_c,
  rain_mm,
  wind_kmh,
  event_ts
from {{ source('silver','clima') }}
