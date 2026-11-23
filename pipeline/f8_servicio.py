"""Fase 8 - Zona de servicio: indicadores pre-calculados en SQLite (el tablero no calcula nada)."""
import sqlite3

from pyspark.sql import functions as F

from pipeline import config
from pipeline.common import ahora_iso, get_logger, spark_session

log = get_logger("servicio")


def ejecutar() -> dict:
    spark = spark_session("servicio")
    df = spark.read.parquet(f"{config.HDFS_URI}{config.HDFS_CURADA}").cache()
    v, d = F.col("valor_del_contrato"), F.col("es_directa")

    ind_dep = (df.groupBy("departamento").agg(
        F.count("*").alias("contratos"), F.sum(F.when(d, 1).otherwise(0)).alias("contratos_directa"),
        F.sum(v).alias("valor_total"), F.sum(F.when(d, v).otherwise(0.0)).alias("valor_directa"))
        .withColumn("pct_contratos_directa", F.round(100 * F.col("contratos_directa") / F.col("contratos"), 2))
        .withColumn("pct_valor_directa", F.round(100 * F.col("valor_directa") / F.col("valor_total"), 2))
        .orderBy(F.desc("valor_total")))
    tot = df.agg(F.sum(v)).first()[0]
    ind_mod = (df.groupBy(F.coalesce("modalidad", F.lit("Sin dato")).alias("modalidad")).agg(
        F.count("*").alias("contratos"), F.sum(v).alias("valor_total"))
        .withColumn("pct_valor", F.round(100 * F.col("valor_total") / F.lit(tot), 2)).orderBy(F.desc("valor_total")))
    ind_ent = (df.groupBy(F.coalesce("nombre_entidad", F.lit("Sin dato")).alias("entidad"), "departamento").agg(
        F.count("*").alias("contratos"), F.sum(v).alias("valor_total"),
        F.round(100 * F.avg(d.cast("int")), 2).alias("pct_contratos_directa"))
        .orderBy(F.desc("valor_total")).limit(20))
    ind_dia = (df.groupBy("fecha_firma").agg(F.count("*").alias("contratos"), F.sum(v).alias("valor_total"),
                                              F.sum(F.when(d, v).otherwise(0.0)).alias("valor_directa"))
               .withColumn("fecha_firma", F.col("fecha_firma").cast("string")).orderBy("fecha_firma"))
    ind_cubo = (df.groupBy("fecha_firma", "departamento", F.coalesce("modalidad", F.lit("Sin dato")).alias("modalidad"))
                .agg(F.count("*").alias("contratos"), F.sum(v).alias("valor_total"))
                .withColumn("fecha_firma", F.col("fecha_firma").cast("string")).orderBy("fecha_firma"))
    ind_ent300 = (df.groupBy(F.coalesce("nombre_entidad", F.lit("Sin dato")).alias("entidad"), "departamento", "orden").agg(
        F.count("*").alias("contratos"), F.sum(v).alias("valor_total"), F.sum(F.when(d, v).otherwise(0.0)).alias("valor_directa"))
        .withColumn("pct_valor_directa", F.round(100 * F.col("valor_directa") / F.col("valor_total"), 2))
        .orderBy(F.desc("valor_total")).limit(300))
    n, corte = df.count(), df.agg(F.max("fecha_de_firma")).first()[0]
    tablas = {"ind_directa_por_departamento": ind_dep, "ind_modalidad": ind_mod,
              "ind_top_entidades": ind_ent, "ind_serie_diaria": ind_dia,
              "ind_cubo_dia_dep_modalidad": ind_cubo, "ind_entidades_top300": ind_ent300}

    config.SERVICIO_DB.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(config.SERVICIO_DB)
    for nombre, sdf in tablas.items():  # "replace" => idempotente: la tabla refleja siempre la curada actual
        sdf.toPandas().to_sql(nombre, con, if_exists="replace", index=False)
        log.info("%s: %s filas", nombre, sdf.count())
    import pandas as pd
    pd.DataFrame([{"fuente": f"datos.gov.co - SECOP II Contratos Electrónicos ({config.DATASET})",
                   "url": f"https://www.datos.gov.co/resource/{config.DATASET}.json",
                   "fecha_corte": str(corte), "filas_curada": n, "generado": ahora_iso()}]
                 ).to_sql("meta_corte", con, if_exists="replace", index=False)
    con.commit()
    con.close()
    spark.stop()
    return {"filas_curada": n, "fecha_corte": str(corte)}


if __name__ == "__main__":
    ejecutar()
