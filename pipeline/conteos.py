"""Conteos por zona (raw local, Kafka, HDFS, curada, servicio, streaming) para demostrar idempotencia."""
import sqlite3

from confluent_kafka import Consumer, TopicPartition

from pipeline import config
from pipeline.common import archivos_raw, hdfs_archivos_raw, hdfs_client


def raw_local() -> int:
    return sum(sum(1 for _ in a.open(encoding="utf-8")) for a in archivos_raw())


def kafka_mensajes() -> int:
    c = Consumer({"bootstrap.servers": config.KAFKA_BOOTSTRAP, "group.id": "contador-no-commit"})
    total = 0
    try:
        for p in range(config.PARTITIONS):
            lo, hi = c.get_watermark_offsets(TopicPartition(config.TOPIC, p), timeout=10)
            total += hi - lo
    finally:
        c.close()
    return total


def hdfs_lineas() -> int:
    client = hdfs_client()
    n = 0
    for ruta in hdfs_archivos_raw(client):
        with client.read(ruta, chunk_size=1 << 20) as r:
            n += sum(t.count(b"\n") for t in r)
    return n


def servicio() -> dict:
    con = sqlite3.connect(config.SERVICIO_DB)
    out = {}
    try:
        out["curada_filas(meta_corte)"] = con.execute("SELECT filas_curada FROM meta_corte").fetchone()[0]
        out["batch_dias"] = con.execute("SELECT COUNT(*) FROM ind_serie_diaria").fetchone()[0]
        out["batch_contratos_en_serie"] = con.execute("SELECT SUM(contratos) FROM ind_serie_diaria").fetchone()[0]
        out["rapida_ventanas"] = con.execute("SELECT COUNT(*) FROM rapida_ventana_diaria").fetchone()[0]
        out["rapida_contratos"] = con.execute("SELECT SUM(contratos) FROM rapida_ventana_diaria").fetchone()[0]
    except sqlite3.Error as e:
        out["error"] = str(e)
    finally:
        con.close()
    return out


def snapshot() -> dict:
    return {"raw_local_lineas": raw_local(), "kafka_mensajes": kafka_mensajes(),
            "hdfs_lineas": hdfs_lineas(), **servicio()}


if __name__ == "__main__":
    import json
    print(json.dumps(snapshot(), indent=2, ensure_ascii=False))
