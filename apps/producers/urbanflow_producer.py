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

    # ----------------------------
    # 1) Backfill (últimos N dias)
    # ----------------------------
    if args.modo in ["retroativo", "misto"]:
        now0 = datetime.now(timezone.utc)
        start = (now0 - timedelta(days=args.dias)).replace(minute=0, second=0, microsecond=0)
        end = now0.replace(minute=0, second=0, microsecond=0)

        ts = start
        while ts <= end:
            publish_at(ts)
            ts += timedelta(hours=1)

        print(f"[OK] backfill concluído: {utc_iso(start)} até {utc_iso(end)}")

    # ----------------------------
    # 2) Realtime infinito
    # ----------------------------
    if args.modo in ["realtime", "misto"]:
        while True:
            publish_at(datetime.now(timezone.utc))
            time.sleep(args.taxa_seg)

if __name__ == "__main__":
    main()