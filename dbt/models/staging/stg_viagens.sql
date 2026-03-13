select
    status,
    fonte,
    coalesce(qtd_registros, 0) as qtd_registros,
    coalesce(qtd_viagens_distintas, 0) as qtd_viagens_distintas,
    coalesce(qtd_passageiros_distintos, 0) as qtd_passageiros_distintos,
    coalesce(qtd_motoristas_distintos, 0) as qtd_motoristas_distintos,
    coalesce(preco_medio, 0) as preco_medio,
    coalesce(preco_total, 0) as preco_total,
    gold_process_ts,
    hora,
    ano,
    mes,
    dia
from {{ source('urbanflow_gold', 'VIAGENS_RESUMO_HORA') }}