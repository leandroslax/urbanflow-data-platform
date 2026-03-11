{{ config(materialized='view') }}

select *
from {{ source('urbanflow_gold','CLIMA_RESUMO_HORA') }}