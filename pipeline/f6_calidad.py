"""Fase 6 - Validaciones automáticas sobre la zona curada. Si falla una CRÍTICA el pipeline se detiene (exit 1)."""
import sys

from pyspark.sql import functions as F

from pipeline import config
from pipeline.common import ahora_iso, escribir_json, get_logger, leer_json, spark_session

log = get_logger("calidad")


def ejecutar() -> dict:
    spark = spark_session("calidad")
    df = spark.read.parquet(f"{config.HDFS_URI}{config.HDFS_CURADA}")
    stats = leer_json(config.CALIDAD_DIR / "curada_stats.json", {})
    n = df.count()
    nulos = {c: df.filter(F.col(c).isNull()).count() for c in ["id_registro", "fecha_de_firma", "valor_del_contrato"]}
    duplicados = n - df.select("id_registro").distinct().count()
    fuera_rango = df.filter(~F.col("valor_del_contrato").between(0, config.VALOR_MAX)).count()
    descartadas = stats.get("descartadas_total", 0)
    pct_desc = 100 * descartadas / max(stats.get("filas_deduplicadas", 1), 1)
    min_f, max_f = df.agg(F.min("fecha_de_firma"), F.max("fecha_de_firma")).first()

    checks = [
        {"nombre": "conteo_mayor_que_cero", "critica": True, "ok": n > 0, "detalle": f"{n} filas"},
        {"nombre": "sin_nulos_en_columnas_clave", "critica": True, "ok": sum(nulos.values()) == 0, "detalle": nulos},
        {"nombre": "sin_id_registro_repetidos", "critica": True, "ok": duplicados == 0, "detalle": f"{duplicados} repetidos"},
        {"nombre": "valor_dentro_de_rango_0_a_1e13", "critica": True, "ok": fuera_rango == 0, "detalle": f"{fuera_rango} fuera de rango"},
        {"nombre": "descartados_menos_del_5_pct", "critica": False, "ok": pct_desc < 5, "detalle": f"{pct_desc:.2f}% descartado"},
    ]
    reporte = {"generado": ahora_iso(), "filas_curada": n, "fecha_firma_min": str(min_f), "fecha_firma_max": str(max_f),
               "filas_raw": stats.get("filas_raw"), "filas_descartadas": descartadas,
               "descartes_por_razon": stats.get("descartes_por_razon"), "validaciones": checks}
    escribir_json(config.CALIDAD_DIR / "reporte_calidad.json", reporte)
    config.EVIDENCIAS_DIR.mkdir(parents=True, exist_ok=True)
    escribir_json(config.EVIDENCIAS_DIR / "reporte_calidad.json", reporte)
    for c in checks:
        log.info("[%s] %s%s -> %s", "OK" if c["ok"] else "FALLA", c["nombre"], " (crítica)" if c["critica"] else "", c["detalle"])
    spark.stop()
    if any(c["critica"] and not c["ok"] for c in checks):
        log.error("validación crítica falló: el pipeline se detiene")
        sys.exit(1)
    return reporte


if __name__ == "__main__":
    ejecutar()
