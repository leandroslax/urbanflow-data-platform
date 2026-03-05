#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import json
import os
import random
import subprocess
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any

import urllib.request
import urllib.parse


# ------------------------
# Utils
# ------------------------
def utc_iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def run_cmd(cmd: List[str], input_text: str, timeout_s: int = 30) -> None:
    """
    Executa um comando e manda input_text via stdin.
    Garante fechamento do stdin e erro explícito se retorno != 0.
    """
    p = subprocess.Popen(
        cmd,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )
    try:
        out, err = p.communicate(input=input_text, timeout=timeout_s)
    except subprocess.TimeoutExpired:
        p.kill()
        out, err = p.communicate()
        raise RuntimeError(
            f"[TIMEOUT] comando travou ({timeout_s}s): {' '.join(cmd)}\nstdout:\n{out}\nstderr:\n{err}"
        )

    if p.returncode != 0:
        raise RuntimeError(
            f"[ERRO] comando retornou {p.returncode}: {' '.join(cmd)}\nstdout:\n{out}\nstderr:\n{err}"
        )


def kafka_produce_lines(topic: str, lines: List[str], bootstrap: str, client_props: str, kafka_bin: str) -> None:
    """
    Publica uma lista de linhas JSON no tópico usando kafka-console-producer.
    Importante: manda tudo via stdin e encerra (communicate).
    """
    if not lines:
        return

    producer_script = os.path.join(kafka_bin, "kafka-console-producer.sh")
    cmd = [
        producer_script,
        "--bootstrap-server", bootstrap,
        "--producer.config", client_props,
        "--topic", topic
    ]

    payload = "\n".join(lines) + "\n"
    run_cmd(cmd, payload, timeout_s=60)


# ------------------------
# Geradores simulados
# ------------------------
def gen_gps(ts: datetime) -> Dict[str, Any]:
    return {
        "ts_evento": utc_iso(ts),
        "id_motorista": str(uuid.uuid4()),
        "latitude": round(-23.55 + random.uniform(-0.1, 0.1), 6),
        "longitude": round(-46.63 + random.uniform(-0.1, 0.1), 6),
        "velocidade": round(random.uniform(0, 120), 2),
        "fonte": "simulado"
    }


def gen_viagem(ts: datetime) -> Dict[str, Any]:
    return {
        "ts_evento": utc_iso(ts),
        "id_viagem": str(uuid.uuid4()),
        "id_motorista": str(uuid.uuid4()),
        "id_passageiro": str(uuid.uuid4()),
        "origem_lat": round(-23.55 + random.uniform(-0.1, 0.1), 6),
        "origem_lon": round(-46.63 + random.uniform(-0.1, 0.1), 6),
        "destino_lat": round(-23.55 + random.uniform(-0.1, 0.1), 6),
        "destino_lon": round(-46.63 + random.uniform(-0.1, 0.1), 6),
        "preco": round(random.uniform(8, 120), 2),
        "moeda": "BRL",
        "status": random.choice(["criada", "em_andamento", "concluida"]),
        "fonte": "simulado"
    }


def gen_incidente(ts: datetime) -> Dict[str, Any]:
    return {
        "ts_evento": utc_iso(ts),
        "id_incidente": str(uuid.uuid4()),
        "tipo": random.choice(["acidente", "bloqueio", "pane", "chuva_forte"]),
        "gravidade": random.choice(["baixa", "media", "alta"]),
        "latitude": round(-23.55 + random.uniform(-0.1, 0.1), 6),
        "longitude": round(-46.63 + random.uniform(-0.1, 0.1), 6),
        "fonte": "simulado"
    }


def gen_clima(ts: datetime) -> Dict[str, Any]:
    return {
        "ts_evento": utc_iso(ts),
        "estado": "SP",
        "cidade": "São Paulo",
        "latitude": round(-23.55 + random.uniform(-0.1, 0.1), 6),
        "longitude": round(-46.63 + random.uniform(-0.1, 0.1), 6),
        "clima": random.choice(["sol", "nublado", "chuva"]),
        "fonte": "simulado"
    }


# ------------------------
# Tráfego TomTom
# ------------------------
def load_traffic_regions(path: str) -> List[Dict[str, Any]]:
    """Carrega lista de regiões (pontos) para coletar tráfego."""
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, list):
        raise ValueError("traffic_regions.json deve ser uma lista de objetos")
    for r in data:
        for k in ["city", "region_id", "label", "lat", "lon"]:
            if k not in r:
                raise ValueError(f"Região inválida (faltando '{k}'): {r}")
    return data


def tomtom_flow_segment(point_lat: float, point_lon: float, api_key: str, zoom: int = 10, timeout_s: int = 10) -> Dict[str, Any]:
    """
    Consulta TomTom Traffic Flow (flowSegmentData) para um ponto (lat/lon).
    Retorna um dict com campos normalizados; em erro, retorna status=error.
    """
    base_url = f"https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/{zoom}/json"
    params = {"point": f"{point_lat},{point_lon}", "key": api_key}
    url = base_url + "?" + urllib.parse.urlencode(params)
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "urbanflow/1.0"})
        with urllib.request.urlopen(req, timeout=timeout_s) as resp:
            raw = resp.read().decode("utf-8")
        j = json.loads(raw)
        fs = j.get("flowSegmentData", {}) if isinstance(j, dict) else {}
        return {
            "status": "ok",
            "currentSpeedKmh": fs.get("currentSpeed"),
            "freeFlowSpeedKmh": fs.get("freeFlowSpeed"),
            "currentTravelTimeSec": fs.get("currentTravelTime"),
            "freeFlowTravelTimeSec": fs.get("freeFlowTravelTime"),
            "confidence": fs.get("confidence"),
            "roadClosure": fs.get("roadClosure"),
        }
    except Exception as e:
        return {"status": "error", "error_message": str(e)}


def gen_trafego(ts_evento: datetime, region: Dict[str, Any], api_key: str) -> Dict[str, Any]:
    """Gera um evento de tráfego (TomTom) para uma região."""
    lat = float(region["lat"])
    lon = float(region["lon"])
    flow = tomtom_flow_segment(lat, lon, api_key)

    evt = {
        "event_id": str(uuid.uuid4()),
        "tipo": "trafego",
        "provider": "tomtom",
        "city": region["city"],
        "region_id": region["region_id"],
        "label": region.get("label", region["region_id"]),
        "point_lat": lat,
        "point_lon": lon,
        "ts_evento": utc_iso(ts_evento),
        "ts_coleta": utc_iso(datetime.now(timezone.utc)),
    }
    evt.update(flow)

    # Métrica derivada útil (para Silver/QuickSight)
    try:
        ctt = evt.get("currentTravelTimeSec")
        ftt = evt.get("freeFlowTravelTimeSec")
        if isinstance(ctt, (int, float)) and isinstance(ftt, (int, float)) and ftt and ftt > 0:
            evt["congestion_ratio"] = float(ctt) / float(ftt)
        else:
            evt["congestion_ratio"] = None
    except Exception:
        evt["congestion_ratio"] = None

    return evt


# ------------------------
# Publish
# ------------------------
def publish_at(ts: datetime, args, tick_i: int = 0) -> None:
    gps = [gen_gps(ts) for _ in range(args.gps_por_tick)]
    viagens = [gen_viagem(ts) for _ in range(args.viagens_por_tick)]
    incidentes = [gen_incidente(ts) for _ in range(args.incidentes_por_tick)]
    clima = [gen_clima(ts) for _ in range(args.clima_por_tick)]

    trafego: List[Dict[str, Any]] = []
    if args.trafego_enabled and (tick_i % max(1, args.trafego_a_cada_ticks) == 0):
        for r in args._traffic_regions:
            trafego.append(gen_trafego(ts, r, args.tomtom_key))

    kafka_produce_lines(
        args.topic_gps,
        [json.dumps(x, ensure_ascii=False) for x in gps],
        args.bootstrap,
        args.client_props,
        args.kafka_bin
    )

    kafka_produce_lines(
        args.topic_viagens,
        [json.dumps(x, ensure_ascii=False) for x in viagens],
        args.bootstrap,
        args.client_props,
        args.kafka_bin
    )

    kafka_produce_lines(
        args.topic_incidentes,
        [json.dumps(x, ensure_ascii=False) for x in incidentes],
        args.bootstrap,
        args.client_props,
        args.kafka_bin
    )

    kafka_produce_lines(
        args.topic_clima,
        [json.dumps(x, ensure_ascii=False) for x in clima],
        args.bootstrap,
        args.client_props,
        args.kafka_bin
    )

    if trafego:
        kafka_produce_lines(
            args.topic_trafego,
            [json.dumps(x, ensure_ascii=False) for x in trafego],
            args.bootstrap,
            args.client_props,
            args.kafka_bin
        )

    print(
        f"[OK] publicado ts={utc_iso(ts)} "
        f"gps={len(gps)} viagens={len(viagens)} incidentes={len(incidentes)} clima={len(clima)} trafego={len(trafego)}",
        flush=True
    )


# ------------------------
# Main
# ------------------------
def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", required=True)
    ap.add_argument("--client-props", required=True)

    ap.add_argument("--kafka-bin", default=os.path.expanduser("~/kafka/bin"))

    ap.add_argument("--topic-gps", default="urbanflow-gps-bruto")
    ap.add_argument("--topic-viagens", default="urbanflow-viagens-bruto")
    ap.add_argument("--topic-incidentes", default="urbanflow-incidentes-bruto")
    ap.add_argument("--topic-clima", default="urbanflow-clima-bruto")
    ap.add_argument("--topic-trafego", default="urbanflow-trafego-bruto")

    # Flags de tráfego (mas agora auto-enable com env key)
    ap.add_argument("--trafego-enabled", action="store_true", help="Habilita coleta de tráfego via TomTom (auto se TOMTOM_API_KEY existir)")
    ap.add_argument("--tomtom-key", default=os.getenv("TOMTOM_API_KEY", ""), help="API key TomTom (ou env TOMTOM_API_KEY)")
    ap.add_argument(
        "--traffic-regions",
        default=os.path.join(os.path.dirname(__file__), "..", "..", "config", "traffic_regions.json"),
        help="Arquivo JSON com regiões (pontos) para coleta de tráfego"
    )
    ap.add_argument("--trafego-a-cada-ticks", type=int, default=1, help="Coleta tráfego a cada N ticks (reduz chamadas)")

    ap.add_argument("--modo", choices=["retroativo", "realtime", "misto"], default="misto")
    ap.add_argument("--dias", type=int, default=30)
    ap.add_argument("--taxa-seg", type=int, default=5)

    ap.add_argument("--gps-por-tick", type=int, default=50)
    ap.add_argument("--viagens-por-tick", type=int, default=10)
    ap.add_argument("--incidentes-por-tick", type=int, default=2)
    ap.add_argument("--clima-por-tick", type=int, default=2)

    args = ap.parse_args()

    # --- Tráfego: auto-enable se TOMTOM_API_KEY existir ---
    env_key = os.getenv("TOMTOM_API_KEY", "").strip()

    if not args.tomtom_key:
        args.tomtom_key = env_key

    if not args.trafego_enabled and args.tomtom_key.strip():
        args.trafego_enabled = True

    if args.trafego_enabled and not args.tomtom_key.strip():
        args.trafego_enabled = False
        print("[WARN] tráfego desabilitado: TOMTOM_API_KEY não definido (ou vazio).", flush=True)

    if args.trafego_enabled:
        args._traffic_regions = load_traffic_regions(args.traffic_regions)
        print(
            f"[OK] tráfego habilitado (TomTom). regioes={len(args._traffic_regions)} "
            f"a_cada_ticks={args.trafego_a_cada_ticks} topic={args.topic_trafego}",
            flush=True
        )
    else:
        args._traffic_regions = []
        print("[OK] tráfego desabilitado (TOMTOM_API_KEY ausente).", flush=True)

    # sanity checks
    for f in [args.client_props]:
        if not os.path.exists(f):
            raise SystemExit(f"[ERRO] arquivo não existe: {f}")

    producer_script = os.path.join(args.kafka_bin, "kafka-console-producer.sh")
    if not os.path.exists(producer_script):
        raise SystemExit(f"[ERRO] kafka-console-producer.sh não encontrado em: {producer_script}")

    # Backfill
    if args.modo in ["retroativo", "misto"]:
        now = datetime.now(timezone.utc)
        start = now - timedelta(days=args.dias)

        ts = start.replace(minute=0, second=0, microsecond=0)
        end = now.replace(minute=0, second=0, microsecond=0)

        tick_i = 0
        while ts <= end:
            publish_at(ts, args, tick_i=tick_i)
            ts += timedelta(hours=1)
            tick_i += 1

    # Realtime loop
    if args.modo in ["realtime", "misto"]:
        tick_i = 0
        while True:
            publish_at(datetime.now(timezone.utc), args, tick_i=tick_i)
            tick_i += 1
            time.sleep(args.taxa_seg)


if __name__ == "__main__":
    main()