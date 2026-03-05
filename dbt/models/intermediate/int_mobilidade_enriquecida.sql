with v as (select * from {{ ref('stg_viagens') }}),
t as (select * from {{ ref('stg_trafego') }}),
c as (select * from {{ ref('stg_clima') }})
select
  v.trip_id,
  v.city,
  v.region,
  v.start_ts,
  v.end_ts,
  v.driver_id,
  v.passenger_id,
  t.congestion_index,
  t.travel_time_seconds,
  t.free_flow_time_seconds,
  c.temperature_c,
  c.rain_mm,
  c.wind_kmh
from v
left join t on v.city = t.city and v.region = t.region
left join c on v.city = c.city and v.region = c.region
