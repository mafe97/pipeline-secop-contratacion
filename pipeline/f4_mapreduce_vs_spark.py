"""Fase 4 - MapReduce (mapper/reducer en Python) vs Spark sobre el raw de HDFS.

Cálculo: número de contratos y valor total por departamento. Dos formas de ejecutar el MapReduce:
  * modo "local" (por defecto, dentro de run_pipeline): flujo de Hadoop Streaming  cat <raw> | mapper | sort | reducer
    leyendo HDFS por WebHDFS.
  * modo "hadoop" (--hadoop): lee la salida de un JOB REAL de Hadoop Streaming (tools/mapreduce_hadoop.sh) que corrió
    en el contenedor del NameNode con hadoop-streaming-3.2.1.jar; ese job escribe en /datalake/mapreduce_out.
"""
import json
import subprocess
import sys
from pathlib import Path

from pipeline import config
from pipeline.common import escribir_json, get_logger, hdfs_archivos_raw, hdfs_client, spark_session

log = get_logger("mapreduce")
MR = Path(__file__).parent / "mapreduce"
HADOOP_OUT = "/datalake/mapreduce_out"


def mapreduce() -> dict:
    client = hdfs_client()
    mapper = subprocess.Popen([sys.executable, str(MR / "mapper.py")], stdin=subprocess.PIPE,
                              stdout=subprocess.PIPE)
    ordenar = subprocess.Popen(["sort"], stdin=mapper.stdout, stdout=subprocess.PIPE,
                               env={"LC_ALL": "C", "PATH": "/usr/bin:/bin"})
    reducer = subprocess.Popen([sys.executable, str(MR / "reducer.py")], stdin=ordenar.stdout,
                               stdout=subprocess.PIPE)
    mapper.stdout.close()
    ordenar.stdout.close()
    for ruta in hdfs_archivos_raw(client):
        with client.read(ruta, chunk_size=1 << 20) as lector:
            for trozo in lector:
                mapper.stdin.write(trozo)
    mapper.stdin.close()
    salida = reducer.communicate()[0].decode("utf-8")
    res = {}
    for linea in salida.splitlines():
        dep, n, total = linea.split("\t")
        res[dep] = {"contratos": int(n), "valor": float(total)}
    return res


def mapreduce_hadoop() -> dict:
    """Lee la salida (part-*) del job de Hadoop Streaming ya ejecutado en el clúster."""
    client = hdfs_client()
    res = {}
    for nombre in client.list(HADOOP_OUT):
        if not nombre.startswith("part-"):
            continue
        with client.read(f"{HADOOP_OUT}/{nombre}", encoding="utf-8") as r:
            for linea in r.read().splitlines():
                dep, n, total = linea.split("\t")
                res[dep] = {"contratos": int(n), "valor": float(total)}
    return res


def spark_calc() -> dict:
    from pyspark.sql import functions as F
    spark = spark_session("mr-vs-spark")
    df = spark.read.json(f"{config.HDFS_URI}{config.HDFS_RAW}/fecha_ingesta=*/*.jsonl")
    d = F.trim(F.col("payload.departamento"))
    dep = F.when(d.isNull() | (d == ""), "SIN DATO").otherwise(d)
    filas = (df.select(dep.alias("dep"), F.col("payload.valor_del_contrato").cast("double").alias("v"))
               .groupBy("dep").agg(F.count("*").alias("n"), F.sum("v").alias("valor")).collect())
    spark.stop()
    return {r["dep"]: {"contratos": r["n"], "valor": float(r["valor"] or 0.0)} for r in filas}


def ejecutar(modo: str = "local") -> dict:
    mr, sp = (mapreduce_hadoop() if modo == "hadoop" else mapreduce()), spark_calc()
    difs = []
    for k in sorted(set(mr) | set(sp)):
        a, b = mr.get(k), sp.get(k)
        if not a or not b or a["contratos"] != b["contratos"] or \
                abs(a["valor"] - b["valor"]) > max(1.0, 1e-9 * abs(b["valor"])):
            difs.append(k)
    lineas = [f"MapReduce ({'job real de Hadoop Streaming en el clúster' if modo == 'hadoop' else 'tubería mapper | sort | reducer sobre HDFS'}) vs Spark"]
    lineas += [f"{'departamento':45} {'MR contratos':>13} {'Spark contr.':>13} {'MR valor':>20} {'Spark valor':>20}"]
    for k in sorted(sp, key=lambda x: -sp[x]["contratos"]):
        a, b = mr.get(k, {"contratos": 0, "valor": 0}), sp[k]
        lineas.append(f"{k[:45]:45} {a['contratos']:>13} {b['contratos']:>13} {a['valor']:>20,.0f} {b['valor']:>20,.0f}")
    tot_mr = sum(v["contratos"] for v in mr.values())
    tot_sp = sum(v["contratos"] for v in sp.values())
    lineas.append(f"\nTOTAL contratos  MapReduce={tot_mr}  Spark={tot_sp}")
    lineas.append(f"Departamentos con diferencias: {difs or 'ninguno'}  ->  "
                  f"{'COINCIDEN' if not difs else 'NO COINCIDEN'}")
    texto = "\n".join(lineas)
    print(texto)
    config.EVIDENCIAS_DIR.mkdir(parents=True, exist_ok=True)
    sufijo = "_hadoop" if modo == "hadoop" else ""
    (config.EVIDENCIAS_DIR / f"mapreduce_vs_spark{sufijo}.txt").write_text(texto + "\n", encoding="utf-8")
    escribir_json(config.CALIDAD_DIR / f"mapreduce_vs_spark{sufijo}.json", {"mapreduce": mr, "spark": sp, "difs": difs})
    if difs:
        raise SystemExit("MapReduce y Spark no coinciden")
    return {"total": tot_sp}


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--hadoop", action="store_true", help="compara contra la salida del job real de Hadoop Streaming")
    ejecutar("hadoop" if ap.parse_args().hadoop else "local")
