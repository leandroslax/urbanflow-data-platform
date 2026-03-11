select
    ano,
    mes,
    dia,
    sum(qtd_registros) as total_registros,
    sum(qtd_viagens_distintas) as total_viagens,
    sum(qtd_passageiros_distintos) as total_passageiros,
    sum(qtd_motoristas_distintos) as total_motoristas,
    sum(preco_total) as receita_total,
    avg(preco_medio) as preco_medio
from {{ ref('stg_viagens') }}
group by 1, 2, 3