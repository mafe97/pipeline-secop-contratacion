"""DAG opcional de Airflow: ejecuta el pipeline completo con reintentos (una tarea por fase)."""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {"retries": 2, "retry_delay": timedelta(minutes=2)}

with DAG("secop_pipeline", start_date=datetime(2026, 11, 1), schedule="0 7 * * *", catchup=False,
         default_args=default_args, tags=["secop"]) as dag:
    pasos = [("ingesta", "f2_ingesta"), ("productor", "f3_kafka_producer"), ("consumidor_hdfs", "f3_kafka_consumer"),
             ("mapreduce_vs_spark", "f4_mapreduce_vs_spark"), ("curada", "f5_curada"), ("calidad", "f6_calidad"),
             ("servicio", "f8_servicio"), ("streaming", "f7_streaming")]
    tareas = [BashOperator(task_id=t, bash_command=f"cd /app && python -m pipeline.{m}") for t, m in pasos]
    for a, b in zip(tareas, tareas[1:]):
        a >> b
