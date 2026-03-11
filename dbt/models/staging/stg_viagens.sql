select
    status,
    fonte,
    qtd_registros,
    qtd_viagens_distintas,
    qtd_passageiros_distintos,
    qtd_motoristas_distintos,
    preco_medio,
    preco_total,
    gold_process_ts,
    hora,
    ano,
    mes,
    dia
from {{ source('urbanflow_gold', 'VIAGENS_RESUMO_HORA') }}