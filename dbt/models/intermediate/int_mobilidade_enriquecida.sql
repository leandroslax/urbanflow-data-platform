{{ config(materialized='view') }}

with viagens as (
    select
        cast(gold_process_ts as date) as data_ref,
        sum(qtd_viagens_distintas) as total_viagens,
        sum(preco_total) as faturamento
    from {{ ref('stg_viagens') }}
    group by 1
),
clima as (
    select
        cast(gold_process_ts as date) as data_ref,
        avg(temperatura_media) as temperatura_media
    from {{ ref('stg_clima') }}
    group by 1
),
trafego as (
    select
        cast(gold_process_ts as date) as data_ref,
        avg(velocidade_media) as velocidade_media_trafego
    from {{ ref('stg_trafego') }}
    group by 1
)

select
    v.data_ref as data,
    v.total_viagens,
    v.faturamento,
    c.temperatura_media,
    t.velocidade_media_trafego
from viagens v
left join clima c on v.data_ref = c.data_ref
left join trafego t on v.data_ref = t.data_ref