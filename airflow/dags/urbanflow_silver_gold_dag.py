from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="urbanflow_silver_gold",
    start_date=datetime(2026, 1, 1),
    schedule_interval="@hourly",
    catchup=False,
) as dag:

    silver_viagens = BashOperator(task_id="silver_viagens", bash_command="scripts/silver/start_silver_viagens.sh")
    silver_gps = BashOperator(task_id="silver_gps", bash_command="scripts/silver/start_silver_gps.sh")
    silver_incidentes = BashOperator(task_id="silver_incidentes", bash_command="scripts/silver/start_silver_incidentes.sh")
    silver_clima = BashOperator(task_id="silver_clima", bash_command="scripts/silver/start_silver_clima.sh")
    silver_trafego = BashOperator(task_id="silver_trafego", bash_command="scripts/silver/start_silver_trafego.sh")

    gold_cong = BashOperator(task_id="gold_congestionamento", bash_command="scripts/gold/start_gold_congestionamento_por_hora.sh")
    gold_inc = BashOperator(task_id="gold_incidentes", bash_command="scripts/gold/start_gold_incidentes_por_regiao.sh")
    gold_tmv = BashOperator(task_id="gold_tempo_medio_viagem", bash_command="scripts/gold/start_gold_tempo_medio_viagem.sh")

    dbt_run = BashOperator(task_id="dbt_run", bash_command="cd dbt && dbt run")
    dbt_test = BashOperator(task_id="dbt_test", bash_command="cd dbt && dbt test")

    [silver_viagens, silver_gps, silver_incidentes, silver_clima, silver_trafego] >> [gold_cong, gold_inc, gold_tmv] >> dbt_run >> dbt_test
