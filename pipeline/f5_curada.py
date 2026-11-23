"""Fase 5 - Zona curada con PySpark: HDFS raw -> limpieza documentada -> Parquet particionado.

Cada problema de datos se mide (filas antes/después) y se guarda en data/calidad/curada_stats.json.
La zona curada se reconstruye de forma determinista desde el raw (que es append-only), por eso
correr el paso dos veces produce exactamente el mismo resultado (idempotente).
"""
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from pipeline import config
from pipeline.common import ahora_iso, escribir_json, get_logger, hdfs_client, spark_session

log = get_logger("curada")

NULOS = ["no definido", "no definida", "sin descripcion", "sin descripción", "n/a", "na", "null", "none", "-", ""]

RAW_SCHEMA = StructType([
    StructField("id_registro", StringType()),
    StructField("fecha_ingesta", StringType()),
    StructField("payload", StructType([StructField(c, StringType()) for c in config.CAMPOS])),
])


def texto_limpio(col: str):
    """trim + colapsa espacios; tokens tipo 'No definido' / '' pasan a NULL."""
    c = F.trim(F.regexp_replace(F.col(col), r"\s+", " "))
    return F.when(F.lower(c).isin(NULOS), F.lit(None)).otherwise(c)


def modalidad_norm(col: str):
    """Unifica variantes: '(con ofertas)', mayúsculas, tildes, 'Licitación pública Obra Publica', etc."""
    base = F.regexp_replace(F.trim(F.col(col)), r"\s*\(.*\)\s*$", "")
    k = F.translate(F.lower(base), "áéíóúü", "aeiouu")
    return (F.when(k.startswith("contratacion directa"), "Contratación directa")
             .when(k.startswith("contratacion regimen especial"), "Contratación régimen especial")
             .when(k.startswith("seleccion abreviada") & k.contains("menor cuantia"), "Selección abreviada de menor cuantía")
             .when(k.startswith("seleccion abreviada") & k.contains("subasta"), "Selección abreviada subasta inversa")
             .when(k.startswith("licitacion publica"), "Licitación pública")
             .when(k.startswith("minima cuantia"), "Mínima cuantía")
             .when(k.startswith("concurso de meritos"), "Concurso de méritos")
             .when(k.startswith("enajenacion de bienes"), "Enajenación de bienes")
             .when(F.lower(base).isin(NULOS), F.lit(None))
             .otherwise(base))


def ejecutar() -> dict:
    spark = spark_session("curada")
    ruta_raw = f"{config.HDFS_URI}{config.HDFS_RAW}/fecha_ingesta=*/*.jsonl"
    raw = spark.read.schema(RAW_SCHEMA).json(ruta_raw).cache()
    stats: dict = {"generado": ahora_iso(), "decisiones": {}}
    n_raw = raw.count()
    stats["filas_raw"] = n_raw
    log.info("filas raw: %s", n_raw)

    # --- 1. Duplicados por id_registro: nos quedamos con la versión más reciente -----------------
    p = raw.select("id_registro", "fecha_ingesta", *[F.col(f"payload.{c}").alias(c) for c in config.CAMPOS])
    orden = Window.partitionBy("id_registro").orderBy(
        F.col("fecha_ingesta").desc(), F.col("ultima_actualizacion").cast("timestamp").desc_nulls_last())
    dedup = p.withColumn("_rn", F.row_number().over(orden)).filter("_rn = 1").drop("_rn")
    n_dedup = dedup.count()
    stats["decisiones"]["duplicados"] = {
        "problema": "El mismo id_registro puede llegar varias veces (reingestas, republicación en Kafka, contratos actualizados).",
        "decision": "Conservar una fila por id_registro: la de mayor fecha_ingesta y, en empate, la de mayor ultima_actualizacion.",
        "por_que": "El raw es un historial; la versión más reciente refleja el estado actual del contrato y evita contar dos veces el dinero.",
        "filas_antes": n_raw, "filas_despues": n_dedup, "eliminadas": n_raw - n_dedup}

    # --- 2. Tipos: fechas y números ---------------------------------------------------------------
    tipado = (dedup
              .withColumn("fecha_de_firma", F.col("fecha_de_firma").cast("timestamp"))
              .withColumn("fecha_de_inicio_del_contrato", F.col("fecha_de_inicio_del_contrato").cast("timestamp"))
              .withColumn("fecha_de_fin_del_contrato", F.col("fecha_de_fin_del_contrato").cast("timestamp"))
              .withColumn("ultima_actualizacion", F.col("ultima_actualizacion").cast("timestamp"))
              .withColumn("valor_del_contrato", F.col("valor_del_contrato").cast("double"))
              .withColumn("valor_pagado", F.col("valor_pagado").cast("double")))
    stats["decisiones"]["tipos"] = {
        "problema": "Socrata entrega todo como texto (fechas ISO y valores numéricos como string).",
        "decision": "fecha_* -> timestamp; valor_* -> double. Lo que no se pueda convertir queda NULL (no se inventa un valor).",
        "por_que": "Las agregaciones por fecha y suma de dinero necesitan tipos reales; NULL explícito es auditable.",
        "fecha_firma_nulas": tipado.filter(F.col("fecha_de_firma").isNull()).count(),
        "valor_nulos": tipado.filter(F.col("valor_del_contrato").isNull()).count()}

    # --- 3. Texto / nulos disfrazados / formatos inconsistentes ----------------------------------
    antes_dep = tipado.filter(F.col("departamento").isin("No Definido", "No definido")).count()
    antes_mod = tipado.select("modalidad_de_contratacion").distinct().count()
    limpio = tipado
    for c in ["departamento", "ciudad", "nombre_entidad", "sector", "orden", "estado_contrato",
              "tipo_de_contrato", "proveedor_adjudicado", "destino_gasto", "origen_de_los_recursos"]:
        limpio = limpio.withColumn(c, texto_limpio(c))
    limpio = (limpio.withColumn("modalidad", modalidad_norm("modalidad_de_contratacion"))
                    .withColumn("departamento", F.coalesce("departamento", F.lit("Sin dato")))
                    .withColumn("es_directa", F.col("modalidad") == "Contratación directa")
                    .withColumn("es_pyme", F.upper(F.trim("es_pyme")) == "SI")
                    .drop("modalidad_de_contratacion"))
    despues_mod = limpio.select("modalidad").distinct().count()
    stats["decisiones"]["texto"] = {
        "problema": "Nulos disfrazados ('No Definido', 'Sin Descripcion', ''), espacios sobrantes y modalidades escritas de varias formas "
                    "('Contratación directa' vs 'Contratación Directa (con ofertas)').",
        "decision": "Nulos disfrazados -> NULL (departamento -> 'Sin dato'); modalidad normalizada a un catálogo de 8 grupos; "
                    "se crea es_directa (bool).",
        "por_que": "Si no se unifican, la misma modalidad se cuenta como categorías distintas y el % de contratación directa se subestima.",
        "departamento_no_definido_antes": antes_dep,
        "modalidades_distintas_antes": antes_mod, "modalidades_distintas_despues": despues_mod}

    # --- 4. Descartes: fila inutilizable para el análisis ----------------------------------------
    razones = {
        "sin_fecha_firma": F.col("fecha_de_firma").isNull(),
        "valor_nulo": F.col("valor_del_contrato").isNull(),
        "valor_negativo": F.col("valor_del_contrato") < 0,
        f"valor_mayor_{config.VALOR_MAX:.0e}": F.col("valor_del_contrato") > config.VALOR_MAX,
    }
    stats["descartes_por_razon"] = {k: limpio.filter(cond).count() for k, cond in razones.items()}
    valido = F.col("fecha_de_firma").isNotNull() & F.col("valor_del_contrato").between(0, config.VALOR_MAX)
    curada = (limpio.filter(valido)
              .withColumn("fecha_firma", F.to_date("fecha_de_firma"))
              .withColumn("anio_mes", F.date_format("fecha_de_firma", "yyyy-MM")))
    n_final = curada.count()
    stats["decisiones"]["descartes"] = {
        "problema": "Filas sin fecha de firma o con valor nulo/negativo/absurdo (> 10 billones COP) no pueden ubicarse en el tiempo ni sumarse.",
        "decision": "Se descartan y se cuentan por razón en el reporte de calidad (no se borran en silencio; el raw conserva todo).",
        "por_que": "Un solo valor absurdo distorsiona totales y promedios por departamento.",
        "filas_antes": n_dedup, "filas_despues": n_final, "descartadas": n_dedup - n_final}
    stats["filas_deduplicadas"], stats["filas_final"], stats["descartadas_total"] = n_dedup, n_final, n_dedup - n_final

    # --- 5. Parquet particionado por anio_mes ----------------------------------------------------
    destino = f"{config.HDFS_URI}{config.HDFS_CURADA}"
    (curada.repartition("anio_mes").write.mode("overwrite").partitionBy("anio_mes").parquet(destino))
    n_part = curada.select("anio_mes").distinct().count()

    client = hdfs_client()
    bytes_raw = client.content(config.HDFS_RAW)["length"]
    bytes_parquet = client.content(config.HDFS_CURADA)["length"]
    stats["particion"] = {
        "columna": "anio_mes", "particiones": n_part,
        "justificacion": "Las consultas del tablero filtran por periodo; el mes da pocas particiones grandes (sin el problema de "
                         "archivos diminutos que tendría el día) y crece de forma predecible. departamento (33 valores, muy sesgado hacia "
                         "Bogotá/Valle/Antioquia) produciría particiones desiguales."}
    stats["tamano"] = {"jsonl_raw_bytes": bytes_raw, "parquet_bytes": bytes_parquet,
                       "reduccion_pct": round(100 * (1 - bytes_parquet / bytes_raw), 1) if bytes_raw else None}
    escribir_json(config.CALIDAD_DIR / "curada_stats.json", stats)
    log.info("curada: %s -> %s -> %s filas | JSONL %.1f MB vs Parquet %.1f MB", n_raw, n_dedup, n_final,
             bytes_raw / 1e6, bytes_parquet / 1e6)
    spark.stop()
    return stats


if __name__ == "__main__":
    ejecutar()
