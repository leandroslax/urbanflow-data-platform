{{ config(materialized='view') }}

select *
from {{ source('urbanflow_gold','VIAGENS_RESUMO_HORA') }}