select
    cidade,
    avg(nivel_congestionamento) as nivel_congestionamento_medio,
    avg(velocidade_media) as velocidade_media_media
from {{ ref('stg_trafego') }}
group by 1