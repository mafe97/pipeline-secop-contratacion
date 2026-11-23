#!/usr/bin/env bash
# Corre el MapReduce como JOB REAL de Hadoop Streaming (hadoop-streaming-3.2.1.jar) en el contenedor del NameNode,
# sobre el raw de HDFS, y compara su salida con Spark. Ejecutar desde el host, con el stack arriba y el raw ya en HDFS.
# El job usa el ejecutor local de MapReduce de Hadoop (no hay YARN en este stack).
set -euo pipefail
mkdir -p evidencias
docker exec secop-namenode bash -c '
  hdfs dfs -rm -r -f -skipTrash /datalake/mapreduce_out >/dev/null 2>&1 || true
  hadoop jar /opt/hadoop-3.2.1/share/hadoop/tools/lib/hadoop-streaming-3.2.1.jar \
    -D mapreduce.framework.name=local -D fs.defaultFS=hdfs://namenode:9000 \
    -files /mr/mapper.py,/mr/reducer.py -mapper "python3 mapper.py" -reducer "python3 reducer.py" \
    -input "/datalake/raw/datos.gov.co/jbjy-vk9h/fecha_ingesta=*" -output /datalake/mapreduce_out 2>&1
  echo; echo "--- hdfs dfs -ls /datalake/mapreduce_out"; hdfs dfs -ls /datalake/mapreduce_out' > /tmp/hadoop_job.log
{ grep -E "Job job_.*running|completed successfully" /tmp/hadoop_job.log | sed -E 's/^[0-9-]+ [0-9:,]+ //' | head -2
  echo "contadores finales del job:"
  awk '/completed successfully/{f=1} f' /tmp/hadoop_job.log | grep -E "^\s+(Map input records|Map output records|Reduce input records|Reduce output records)=" | sed 's/^\s*/  /'
  sed -n '/--- hdfs/,$p' /tmp/hadoop_job.log; } | tee evidencias/hadoop_streaming_job.txt
docker compose exec -T pipeline python -m pipeline.f4_mapreduce_vs_spark --hadoop | tail -4
