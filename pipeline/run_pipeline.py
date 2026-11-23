"""Corre el pipeline completo, en orden. Se detiene en el primer paso que falle (p. ej. validación crítica)."""
import subprocess
import sys

from pipeline.common import get_logger

log = get_logger("pipeline")
PASOS = [
    ("F2 ingesta raw (API -> JSONL)", "pipeline.f2_ingesta"),
    ("F3 productor Kafka", "pipeline.f3_kafka_producer"),
    ("F3/F4 consumidor Kafka -> HDFS", "pipeline.f3_kafka_consumer"),
    ("F4 MapReduce vs Spark", "pipeline.f4_mapreduce_vs_spark"),
    ("F5 zona curada (PySpark -> Parquet)", "pipeline.f5_curada"),
    ("F6 calidad (se detiene si falla una crítica)", "pipeline.f6_calidad"),
    ("F8 zona de servicio (SQLite)", "pipeline.f8_servicio"),
    ("F7 capa rápida (Structured Streaming)", "pipeline.f7_streaming"),
]


def ejecutar() -> None:
    for nombre, modulo in PASOS:
        log.info("=== %s ===", nombre)
        r = subprocess.run([sys.executable, "-m", modulo])
        if r.returncode != 0:
            log.error("el paso '%s' falló (código %s): pipeline detenido", nombre, r.returncode)
            sys.exit(r.returncode)
    log.info("PIPELINE COMPLETO SIN ERRORES")


if __name__ == "__main__":
    ejecutar()
