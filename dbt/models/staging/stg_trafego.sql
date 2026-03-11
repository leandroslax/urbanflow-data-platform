{{ config(materialized='view') }}

select *
from {{ source('urbanflow_gold','TRAFEGO_RESUMO_HORA') }}