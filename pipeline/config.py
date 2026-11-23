"""Configuración central del pipeline SECOP II (todo se puede sobreescribir por variable de entorno)."""
import os
from pathlib import Path

DATA_DIR = Path(os.getenv("DATA_DIR", "data"))
RAW_DIR = DATA_DIR / "raw"
STATE_DIR = DATA_DIR / "state"
CALIDAD_DIR = DATA_DIR / "calidad"
SERVICIO_DB = Path(os.getenv("SERVICIO_DB", str(DATA_DIR / "servicio" / "secop.db")))
CHECKPOINT_DIR = DATA_DIR / "checkpoints" / "stream"
EVIDENCIAS_DIR = Path(os.getenv("EVIDENCIAS_DIR", "evidencias"))

# --- Fuente (Fase 1) ---
FUENTE = "datos.gov.co"
DATASET = "jbjy-vk9h"  # SECOP II - Contratos Electrónicos
SOCRATA_URL = f"https://www.datos.gov.co/resource/{DATASET}.json"
SOCRATA_APP_TOKEN = os.getenv("SOCRATA_APP_TOKEN", "")
COL_FECHA = "fecha_de_firma"
COL_ID = "id_contrato"          # id_registro: el id del contrato es único en el dataset
PAGE_SIZE = int(os.getenv("PAGE_SIZE", "10000"))
INGEST_DESDE = os.getenv("INGEST_DESDE", "2026-10-25T00:00:00")
INGEST_HASTA = os.getenv("INGEST_HASTA", "2026-11-05T00:00:00")
MAX_REGISTROS = int(os.getenv("MAX_REGISTROS", "0"))  # 0 = sin tope (solo para pruebas)
HTTP_REINTENTOS = 5
HTTP_TIMEOUT = 90

# --- Kafka (Fase 3) ---
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = "secop.contratos.raw"
PARTITIONS = 3
GROUP_ID = "secop-hdfs-writer"
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "5000"))      # mensajes por archivo en HDFS
IDLE_SECONDS = int(os.getenv("IDLE_SECONDS", "15"))    # el consumidor termina tras este silencio

# --- HDFS (Fase 4) ---
HDFS_WEB = os.getenv("HDFS_WEB", "http://namenode:9870")
HDFS_URI = os.getenv("HDFS_URI", "hdfs://namenode:9000")
HDFS_RAW = f"/datalake/raw/{FUENTE}/{DATASET}"
HDFS_CURADA = f"/datalake/curada/{FUENTE}/{DATASET}"

# --- Reglas de calidad (Fase 5-6) ---
VALOR_MAX = 1e13  # un contrato > 10 billones de pesos se considera error de digitación

# Campos del registro que se usan aguas abajo (el payload crudo conserva todos)
CAMPOS = [
    "id_contrato", "proceso_de_compra", "nombre_entidad", "nit_entidad", "departamento",
    "ciudad", "orden", "sector", "estado_contrato", "tipo_de_contrato",
    "modalidad_de_contratacion", "fecha_de_firma", "fecha_de_inicio_del_contrato",
    "fecha_de_fin_del_contrato", "valor_del_contrato", "valor_pagado",
    "proveedor_adjudicado", "es_pyme", "destino_gasto", "origen_de_los_recursos",
    "ultima_actualizacion",
]
