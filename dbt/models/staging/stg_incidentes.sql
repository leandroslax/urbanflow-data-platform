select
  incident_id,
  city,
  region,
  incident_type,
  severity,
  description,
  event_ts
from {{ source('silver','incidentes') }}
