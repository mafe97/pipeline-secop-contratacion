"""Utilidades compartidas: sobre JSON, JSONL, estado y cliente HDFS."""
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

from pipeline import config


def get_logger(nombre: str) -> logging.Logger:
    for ruidoso in ("hdfs", "hdfs.client", "py4j", "py4j.clientserver", "urllib3"):
        logging.getLogger(ruidoso).setLevel(logging.WARNING)
    logging.basicConfig(level=logging.INFO, stream=sys.stdout,
                        format="%(asctime)s %(levelname)s [%(name)s] %(message)s")
    return logging.getLogger(nombre)


def ahora_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def construir_sobre(registro: dict, fecha_ingesta: str) -> dict:
    """Sobre JSON: metadatos + el registro original intacto en `payload`."""
    return {
        "fuente": config.FUENTE,
        "dataset": config.DATASET,
        "id_registro": str(registro[config.COL_ID]),
        "fecha_ingesta": fecha_ingesta,
        "payload": registro,
    }


def raw_base() -> Path:
    return config.RAW_DIR / config.FUENTE / config.DATASET


def archivos_raw() -> list[Path]:
    """Archivos JSONL de la zona raw local (sin manifiestos)."""
    return sorted(raw_base().glob("fecha_ingesta=*/*.jsonl"))


def leer_json(ruta: Path, defecto):
    return json.loads(ruta.read_text(encoding="utf-8")) if ruta.exists() else defecto


def escribir_json(ruta: Path, obj) -> None:
    ruta.parent.mkdir(parents=True, exist_ok=True)
    ruta.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


def hdfs_client():
    from hdfs import InsecureClient  # import perezoso: no se necesita en tests
    return InsecureClient(config.HDFS_WEB, user="root")


def hdfs_archivos_raw(client) -> list[str]:
    """Rutas de todos los lotes JSONL en HDFS (zona raw)."""
    rutas = []
    if client.status(config.HDFS_RAW, strict=False) is None:
        return rutas
    for dirpath, _, files in client.walk(config.HDFS_RAW):
        rutas += [f"{dirpath}/{f}" for f in files if f.endswith(".jsonl")]
    return sorted(rutas)


def spark_session(nombre: str, kafka: bool = False):
    from pyspark.sql import SparkSession
    b = (SparkSession.builder.appName(nombre).master("local[*]")
         .config("spark.driver.memory", "3g")
         .config("spark.sql.shuffle.partitions", "8")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.ui.showConsoleProgress", "false")
         .config("spark.sql.sources.partitionOverwriteMode", "dynamic"))
    if kafka:
        b = b.config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3")
    spark = b.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark
