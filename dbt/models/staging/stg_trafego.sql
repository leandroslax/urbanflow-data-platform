select
    fonte,
    cidade,
    gold_process_ts,
    velocidade_media,
    nivel_congestionamento
from {{ source('urbanflow_gold', 'TRAFEGO_RESUMO_HORA') }}