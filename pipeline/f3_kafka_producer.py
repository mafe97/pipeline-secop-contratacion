"""Fase 3 - Productor Kafka: cada línea del raw es un mensaje con key = id_registro.

Evita duplicados con dos capas:
  1) un libro de registro (ledger) de archivos ya publicados: correr dos veces NO republica;
  2) producer idempotente (enable.idempotence) para que reintentos de red no dupliquen.
Si alguien fuerza la republicación (--force), la key idéntica hace que el mismo id caiga siempre en
la misma partición y la zona curada deduplica por id_registro (ver README).
"""
import argparse
import json
import time
from collections import Counter

from confluent_kafka import KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic

from pipeline import config
from pipeline.common import archivos_raw, escribir_json, get_logger, leer_json

log = get_logger("productor")
LEDGER = config.STATE_DIR / "producer_ledger.json"


def asegurar_topico() -> None:
    admin = AdminClient({"bootstrap.servers": config.KAFKA_BOOTSTRAP})
    for _ in range(30):  # Kafka puede tardar unos segundos tras arrancar
        try:
            admin.list_topics(timeout=5)
            break
        except KafkaException:
            time.sleep(2)
    futuros = admin.create_topics([NewTopic(config.TOPIC, num_partitions=config.PARTITIONS,
                                            replication_factor=1)])
    for t, f in futuros.items():
        try:
            f.result()
            log.info("tópico %s creado con %s particiones", t, config.PARTITIONS)
        except KafkaException as e:
            if "TOPIC_ALREADY_EXISTS" in str(e):
                log.info("tópico %s ya existe", t)
            else:
                raise


def ejecutar(force: bool = False) -> dict:
    asegurar_topico()
    ledger = {} if force else leer_json(LEDGER, {})
    pendientes = [a for a in archivos_raw() if str(a) not in ledger]
    if not pendientes:
        log.info("nada nuevo que publicar (todos los archivos ya están en el ledger)")
        return {"publicados": 0, "por_particion": {}}

    productor = Producer({"bootstrap.servers": config.KAFKA_BOOTSTRAP, "enable.idempotence": True,
                          "acks": "all", "linger.ms": 50, "compression.type": "lz4"})
    por_particion, errores = Counter(), []

    def entregado(err, msg):
        if err:
            errores.append(str(err))
        else:
            por_particion[msg.partition()] += 1

    total = 0
    for archivo in pendientes:
        n = 0
        with archivo.open(encoding="utf-8") as f:
            for linea in f:
                key = json.loads(linea)["id_registro"]
                while True:
                    try:
                        productor.produce(config.TOPIC, key=key, value=linea.rstrip("\n"),
                                          on_delivery=entregado)
                        break
                    except BufferError:
                        productor.poll(0.5)
                productor.poll(0)
                n += 1
        productor.flush()
        if errores:
            raise RuntimeError(f"fallaron entregas: {errores[:3]}")
        ledger[str(archivo)] = n
        escribir_json(LEDGER, ledger)  # se anota solo tras flush exitoso
        total += n
        log.info("%s: %s mensajes publicados", archivo.name, n)
    resumen = {"publicados": total, "por_particion": dict(sorted(por_particion.items()))}
    log.info("resumen: %s", resumen)
    return resumen


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--force", action="store_true", help="ignora el ledger y republica todo (demo de duplicados)")
    ejecutar(ap.parse_args().force)
