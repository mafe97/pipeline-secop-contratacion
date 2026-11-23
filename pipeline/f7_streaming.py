"""Fase 7 - Capa rápida: Spark Structured Streaming sobre el tópico de Kafka.

Agregación por ventana de 1 día (event time = fecha_de_firma) con watermark de 7 días y checkpoint.
- dropDuplicates(id_registro, event_time) dentro del watermark => un mensaje republicado no se cuenta dos veces.
- El checkpoint guarda los offsets leídos y el estado de las ventanas: al reiniciar NO se reprocesa.
- La salida se upserta en SQLite (clave = inicio de ventana), así que es idempotente incluso ante reintentos de micro-lote.
Trigger availableNow: procesa todo lo pendiente y termina (apto para orquestar y demostrar reinicios).
"""
import sqlite3

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from pipeline import config
from pipeline.common import ahora_iso, get_logger, spark_session

log = get_logger("streaming")

SCHEMA = StructType([
    StructField("id_registro", StringType()),
    StructField("payload", StructType([StructField("fecha_de_firma", StringType()),
                                       StructField("valor_del_contrato", StringType())])),
])
TABLA = "rapida_ventana_diaria"


def _upsert(batch_df, batch_id: int) -> None:
    filas = [(r["inicio"].strftime("%Y-%m-%d"), int(r["contratos"]), float(r["valor_total"] or 0.0), ahora_iso())
             for r in batch_df.collect()]
    config.SERVICIO_DB.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(config.SERVICIO_DB)
    con.execute(f"CREATE TABLE IF NOT EXISTS {TABLA} (ventana_inicio TEXT PRIMARY KEY, contratos INTEGER, "
                "valor_total REAL, actualizado TEXT)")
    con.executemany(f"INSERT INTO {TABLA} VALUES (?,?,?,?) ON CONFLICT(ventana_inicio) DO UPDATE SET "
                    "contratos=excluded.contratos, valor_total=excluded.valor_total, actualizado=excluded.actualizado", filas)
    con.commit()
    con.close()
    log.info("micro-lote %s: %s ventanas actualizadas", batch_id, len(filas))


def ejecutar() -> dict:
    spark = spark_session("streaming", kafka=True)
    entrada = (spark.readStream.format("kafka").option("kafka.bootstrap.servers", config.KAFKA_BOOTSTRAP)
               .option("subscribe", config.TOPIC).option("startingOffsets", "earliest")
               .option("failOnDataLoss", "false").load())
    ev = (entrada.select(F.from_json(F.col("value").cast("string"), SCHEMA).alias("m"))
          .select("m.id_registro", F.col("m.payload.fecha_de_firma").cast("timestamp").alias("event_time"),
                  F.col("m.payload.valor_del_contrato").cast("double").alias("valor"))
          .filter(F.col("event_time").isNotNull() & F.col("valor").between(0, config.VALOR_MAX)))  # mismas reglas que curada
    agg = (ev.withWatermark("event_time", "7 days").dropDuplicates(["id_registro", "event_time"])
           .groupBy(F.window("event_time", "1 day").alias("w"))
           .agg(F.count("*").alias("contratos"), F.sum("valor").alias("valor_total"))
           .select(F.col("w.start").alias("inicio"), "contratos", "valor_total"))
    q = (agg.writeStream.outputMode("update").foreachBatch(_upsert)
         .option("checkpointLocation", str(config.CHECKPOINT_DIR)).trigger(availableNow=True).start())
    q.awaitTermination()
    ultimo = q.lastProgress or {}
    resumen = {"filas_entrada_ultima_corrida": ultimo.get("numInputRows", 0), "batch_id": ultimo.get("batchId")}
    log.info("fin de la corrida: %s", resumen)
    spark.stop()
    return resumen


if __name__ == "__main__":
    ejecutar()
