#!/usr/bin/env bash
# Genera las evidencias/ (ejecutar DENTRO de `docker compose exec pipeline` salvo los comandos docker, que se lanzan desde el host).
# Uso desde el host:  bash run_evidencias.sh
set -euo pipefail
E=evidencias; mkdir -p $E
X="docker compose exec -T pipeline"

echo "== Kafka: tópico y grupo de consumo =="
{
  docker exec secop-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --describe --topic secop.contratos.raw
  docker exec secop-kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --describe --group secop-hdfs-writer
} | tee $E/kafka_topico_y_consumidor.txt

echo "== HDFS =="
docker exec secop-namenode hdfs dfs -ls -R /datalake | tee $E/hdfs_ls.txt
docker exec secop-namenode hdfs dfs -du -h /datalake | tee -a $E/hdfs_ls.txt

echo "== Streaming: reinicio =="
$X python -m pipeline.demo_streaming 2>&1 | grep -E "Tras arranque|RESULTADO|micro-lote|fin de la corrida"

echo "== Productor dos veces: sin --force no republica; con --force sí, y la curada deduplica =="
{
  echo "--- productor otra vez (sin --force) ---"
  $X python -m pipeline.f3_kafka_producer 2>&1 | grep -E "nada nuevo|resumen"
  echo "--- conteos antes de forzar ---"; $X python -m pipeline.conteos | tr -d '\n '; echo
  echo "--- productor con --force (simula una republicación por error) ---"
  $X python -m pipeline.f3_kafka_producer --force 2>&1 | grep -E "resumen"
  $X python -m pipeline.f3_kafka_consumer 2>&1 | grep -E "resumen"
  $X python -m pipeline.f5_curada 2>&1 | grep -E "INFO \[curada\]"
  $X python -m pipeline.f6_calidad 2>&1 | grep -E "\[calidad\]"
  $X python -m pipeline.f8_servicio >/dev/null 2>&1
  $X python -m pipeline.f7_streaming 2>&1 | grep -E "fin de la corrida"
  echo "--- conteos tras forzar: Kafka y HDFS se duplican, curada/servicio/streaming NO ---"; $X python -m pipeline.conteos | tr -d '\n '; echo
} | tee $E/kafka_duplicados.txt
