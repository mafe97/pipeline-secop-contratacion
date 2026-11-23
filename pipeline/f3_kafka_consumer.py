"""Fase 3/4 - Consumidor Kafka (grupo) -> HDFS, un archivo por lote (no por mensaje).

Estructura: /datalake/raw/<fuente>/<dataset>/fecha_ingesta=AAAA-MM-DD/lote_p<partición>_o<ini>-<fin>.jsonl
- Los lotes se alinean a offsets múltiplos de BATCH_SIZE, por lo que el nombre es determinista y
  reescribir un lote (overwrite) es idempotente.
- Los offsets se confirman DESPUÉS de escribir en HDFS (at-least-once; duplicados -> se resuelven en curada).
"""
from collections import defaultdict

from confluent_kafka import Consumer, TopicPartition

from pipeline import config
from pipeline.common import get_logger, hdfs_client

import json
import time

log = get_logger("consumidor")


def ejecutar() -> dict:
    client = hdfs_client()
    c = Consumer({"bootstrap.servers": config.KAFKA_BOOTSTRAP, "group.id": config.GROUP_ID,
                  "auto.offset.reset": "earliest", "enable.auto.commit": False})
    c.subscribe([config.TOPIC])
    buffers: dict[int, list] = defaultdict(list)  # partición -> [(offset, fecha_ingesta, bytes)]
    archivos, mensajes = 0, 0

    def volcar(p: int) -> None:
        nonlocal archivos
        lote = buffers.pop(p, [])
        if not lote:
            return
        por_fecha = defaultdict(list)
        for off, fecha, valor in lote:
            por_fecha[fecha].append(valor)
        ini, fin = lote[0][0], lote[-1][0]
        for fecha, lineas in por_fecha.items():
            ruta = f"{config.HDFS_RAW}/fecha_ingesta={fecha}/lote_p{p}_o{ini:010d}-{fin:010d}.jsonl"
            client.write(ruta, data=(b"\n".join(lineas) + b"\n"), overwrite=True)
            archivos += 1
            log.info("HDFS <- %s (%s mensajes)", ruta, len(lineas))
        c.commit(offsets=[TopicPartition(config.TOPIC, p, fin + 1)], asynchronous=False)

    ultimo_msg = time.time()
    while time.time() - ultimo_msg < config.IDLE_SECONDS:
        msg = c.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            log.warning("error de consumidor: %s", msg.error())
            continue
        ultimo_msg = time.time()
        mensajes += 1
        valor = msg.value()
        fecha = json.loads(valor)["fecha_ingesta"]
        p = msg.partition()
        buffers[p].append((msg.offset(), fecha, valor))
        if (msg.offset() + 1) % config.BATCH_SIZE == 0:
            volcar(p)
    for p in list(buffers):
        volcar(p)
    c.close()
    resumen = {"mensajes_leidos": mensajes, "archivos_escritos": archivos}
    log.info("resumen: %s", resumen)
    return resumen


if __name__ == "__main__":
    ejecutar()
