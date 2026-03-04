#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import json
import random
import subprocess
import time
import uuid
from datetime import datetime, timedelta, timezone

# ----------------------------
# Config Brasil (capitais + bairros)
# ----------------------------
CIDADES = [
    {
        "estado": "SP", "cidade": "São Paulo",
        "bairros": ["Sé", "Pinheiros", "Moema", "Itaim Bibi", "Vila Mariana"],
        "centro": (-23.5505, -46.6333)
    },
    {
        "estado": "RJ", "cidade": "Rio de Janeiro",
        "bairros": ["Copacabana", "Ipanema", "Tijuca", "Centro", "Barra da Tijuca"],
        "centro": (-22.9068, -43.1729)
    },
    {
        "estado": "MG", "cidade": "Belo Horizonte",
        "bairros": ["Savassi", "Lourdes", "Funcionários", "Pampulha", "Centro"],
        "centro": (-19.9167, -43.9345)
    },
    {
        "estado": "PR", "cidade": "Curitiba",
        "bairros": ["Centro", "Batel", "Água Verde", "Santa Felicidade", "Cabral"],
        "centro": (-25.4284, -49.2733)
    },
    {
        "estado": "RS", "cidade": "Porto Alegre",
        "bairros": ["Centro Histórico", "Moinhos de Vento", "Bom Fim", "Cidade Baixa", "Menino Deus"],
        "centro": (-30.0346, -51.2177)
    },
    {
        "estado": "BA", "cidade": "Salvador",
        "bairros": ["Barra", "Ondina", "Pituba", "Centro", "Itapuã"],
        "centro": (-12.9714, -38.5014)
    },
]

TIPOS_VEICULO = ["carro", "moto", "onibus", "caminhao", "van"]
CONDICOES_CLIMA = [
    {"condicao": "ceu_limpo", "chuva_mm_h": (0.0, 0.0), "visibilidade_m": (8000, 15000), "temp_c": (20, 34)},
    {"condicao": "nublado", "chuva_mm_h": (0.0, 0.3), "visibilidade_m": (5000, 12000), "temp_c": (18, 30)},
    {"condicao": "chuva_fraca", "chuva_mm_h": (0.2, 2.0), "visibilidade_m": (3000, 8000), "temp_c": (17, 28)},
    {"condicao": "chuva_forte", "chuva_mm_h": (2.0, 15.0), "visibilidade_m": (800, 5000), "temp_c": (16, 26)},
    {"condicao": "neblina", "chuva_mm_h": (0.0, 0.0), "visibilidade_m": (200, 1500), "temp_c": (12, 22)},
]

INCIDENTES = ["acidente", "obra", "alagamento", "evento", "manifestacao", "semaforo_off"]
SEVERIDADES = ["baixa", "media", "alta"]

def utc_iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def jitter_latlon(lat: float, lon: float, km: float = 2.0):
    # jitter simples ~ 1km ~ 0.009 deg latitude
    d = km / 111.0
    return (
        lat + random.uniform(-d, d),
        lon + random.uniform(-d, d),
    )

def clima_simulado():
    c = random.choice(CONDICOES_CLIMA)
    return {
        "condicao": c["condicao"],
        "temperatura_c": round(random.uniform(*c["temp_c"]), 1),
        "chuva_mm_h": round(random.uniform(*c["chuva_mm_h"]), 2),
        "visibilidade_m": random.randint(*c["visibilidade_m"]),
    }

def kafka_produce_lines(topic: str, lines: list[str], bootstrap: str, client_props: str, kafka_bin: str):
    cmd = [
        f"{kafka_bin}/kafka-console-producer.sh",
        "--bootstrap-server", bootstrap,
        "--producer.config", client_props,
        "--topic", topic
    ]
    p = subprocess.Popen(cmd, stdin=subprocess.PIPE, text=True)
    try:
        for ln in lines:
            p.stdin.write(ln + "\n")
        p.stdin.flush()
    finally:
        p.stdin.close()
        p.wait(timeout=30)

def gerar_eventos(ts: datetime, qtd_gps: int, qtd_viagens: int, qtd_incidentes: int):
    gps = []
    viagens = []
    incidentes = []

    for _ in range(qtd_gps):
        c = random.choice(CIDADES)
        lat, lon = jitter_latlon(*c["centro"], km=3.0)
        veiculo_id = f"{random.choice(TIPOS_VEICULO)}_{random.randint(1, 200):03d}"

        gps.append({
            "ts_evento": utc_iso(ts),
            "veiculo_id": veiculo_id,
            "tipo_veiculo": veiculo_id.split("_")[0],
            "linha": f"L{random.randint(1, 99):02d}",
            "estado": c["estado"],
            "cidade": c["cidade"],
            "bairro": random.choice(c["bairros"]),
            "latitude": round(lat, 6),
            "longitude": round(lon, 6),
            "velocidade_kmh": round(max(0.0, random.gauss(32, 14)), 1),
            "rumo_graus": random.randint(0, 359),
            "precisao_m": random.randint(3, 25),
            "fonte_gps": random.choice(["mobile", "telemetria", "rastreador"]),
            "status_sinal": random.choice(["ok", "intermitente"]),
            "clima": clima_simulado(),
        })

    for _ in range(qtd_viagens):
        origem = random.choice(CIDADES)
        destino = random.choice(CIDADES)
        o_lat, o_lon = jitter_latlon(*origem["centro"], km=4.0)
        d_lat, d_lon = jitter_latlon(*destino["centro"], km=4.0)
        viagem_id = str(uuid.uuid4())

        ts_inicio = ts - timedelta(minutes=random.randint(0, 25))
        dur_min = random.randint(8, 75)

        status = random.choices(
            ["iniciada", "em_andamento", "finalizada", "cancelada"],
            weights=[10, 55, 30, 5],
            k=1
        )[0]

        motivo = None
        if status == "cancelada":
            motivo = random.choice(["motorista_cancelou", "passageiro_cancelou", "sem_motorista", "falha_app"])

        viagens.append({
            "viagem_id": viagem_id,
            "ts_inicio": utc_iso(ts_inicio),
            "ts_fim_previsto": utc_iso(ts_inicio + timedelta(minutes=dur_min)),
            "estado": origem["estado"],
            "cidade": origem["cidade"],
            "origem": {
                "estado": origem["estado"],
                "cidade": origem["cidade"],
                "bairro": random.choice(origem["bairros"]),
                "latitude": round(o_lat, 6),
                "longitude": round(o_lon, 6),
            },
            "destino": {
                "estado": destino["estado"],
                "cidade": destino["cidade"],
                "bairro": random.choice(destino["bairros"]),
                "latitude": round(d_lat, 6),
                "longitude": round(d_lon, 6),
            },
            "tipo_veiculo": random.choice(TIPOS_VEICULO),
            "distancia_km": round(max(0.5, random.gauss(9.0, 6.0)), 2),
            "duracao_prevista_min": dur_min,
            "ocupacao_pct": random.randint(0, 100),
            "tarifa_rs": round(max(5.0, random.gauss(28.0, 14.0)), 2),
            "status_viagem": status,
            "motivo_cancelamento": motivo,
            "clima": clima_simulado(),
        })

    for _ in range(qtd_incidentes):
        c = random.choice(CIDADES)
        lat, lon = jitter_latlon(*c["centro"], km=5.0)
        incidentes.append({
            "incidente_id": str(uuid.uuid4()),
            "ts_evento": utc_iso(ts),
            "tipo_incidente": random.choice(INCIDENTES),
            "severidade": random.choice(SEVERIDADES),
            "estado": c["estado"],
            "cidade": c["cidade"],
            "bairro": random.choice(c["bairros"]),
            "latitude": round(lat, 6),
            "longitude": round(lon, 6),
            "faixa_interditada": random.choice([True, False]),
            "impacto": random.choice(["leve", "moderado", "alto"]),
            "fonte": random.choice(["cctv", "sensor", "usuario_app", "prefeitura"]),
        })

    return gps, viagens, incidentes

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", required=True)
    ap.add_argument("--client-props", default="/home/ec2-user/client_iam.properties")
    ap.add_argument("--kafka-bin", default="/home/ec2-user/kafka/bin")
    ap.add_argument("--topic-gps", default="urbanflow-gps-bruto")
    ap.add_argument("--topic-viagens", default="urbanflow-viagens-bruto")
    ap.add_argument("--topic-incidentes", default="urbanflow-incidentes-bruto")

    ap.add_argument("--modo", choices=["realtime", "retroativo", "misto"], default="misto")
    ap.add_argument("--dias", type=int, default=30, help="usado no retroativo/misto")
    ap.add_argument("--taxa-seg", type=float, default=1.0, help="sleep entre lotes no realtime/misto")

    ap.add_argument("--gps-por-lote", type=int, default=50)
    ap.add_argument("--viagens-por-lote", type=int, default=10)
    ap.add_argument("--incidentes-por-lote", type=int, default=2)

    args = ap.parse_args()

    now = datetime.now(timezone.utc)

    def publish_at(ts):
        gps, viagens, incidentes = gerar_eventos(
            ts,
            qtd_gps=args.gps_por_lote,
            qtd_viagens=args.viagens_por_lote,
            qtd_incidentes=args.incidentes_por_lote
        )

        kafka_produce_lines(args.topic_gps, [json.dumps(x, ensure_ascii=False) for x in gps],
                            args.bootstrap, args.client_props, args.kafka_bin)
        kafka_produce_lines(args.topic_viagens, [json.dumps(x, ensure_ascii=False) for x in viagens],
                            args.bootstrap, args.client_props, args.kafka_bin)
        kafka_produce_lines(args.topic_incidentes, [json.dumps(x, ensure_ascii=False) for x in incidentes],
                            args.bootstrap, args.client_props, args.kafka_bin)

        print(f"[OK] publicado ts={utc_iso(ts)} gps={len(gps)} viagens={len(viagens)} incidentes={len(incidentes)}")

    if args.modo in ["retroativo", "misto"]:
        start = now - timedelta(days=args.dias)
        # publica retroativo por hora (fica realista e não explode volume)
        ts = start.replace(minute=0, second=0, microsecond=0)
        end = now.replace(minute=0, second=0, microsecond=0)

        while ts <= end:
            publish_at(ts)
            ts += timedelta(hours=1)

    if args.modo in ["realtime", "misto"]:
        while True:
            publish_at(datetime.now(timezone.utc))
            time.sleep(args.taxa_seg)

if __name__ == "__main__":
    main()
