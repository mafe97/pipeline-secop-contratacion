#!/usr/bin/env bash
# Genera evidencias/ desde el host (usa docker exec / docker compose exec).
#   bash run_evidencias.sh listados     -> Kafka (tópico y grupo) y HDFS (hdfs dfs -ls) con el estado actual
#   bash run_evidencias.sh duplicados   -> republica Kafka A PROPÓSITO y muestra que curada/servicio/streaming no duplican
#                                          (deja Kafka/HDFS con duplicados: reiniciar el stack después si se va a demostrar en vivo)
#   bash run_evidencias.sh hadoop       -> MapReduce como job real de Hadoop Streaming vs Spark
set -euo pipefail
E=evidencias; mkdir -p $E
X="docker compose exec -T pipeline"

listados() {
  {
    docker exec secop-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --describe --topic secop.contratos.raw
    docker exec secop-kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --describe --group secop-hdfs-writer
  } | tee $E/kafka_topico_y_consumidor.txt
  { docker exec secop-namenode hdfs dfs -ls -R /datalake; docker exec secop-namenode hdfs dfs -du -h /datalake; } | tee $E/hdfs_ls.txt
}

duplicados() {
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
}

case "${1:-listados}" in
  listados) listados ;;
  duplicados) duplicados ;;
  hadoop) bash tools/mapreduce_hadoop.sh ;;
  *) echo "uso: $0 listados|duplicados|hadoop"; exit 1 ;;
esac
