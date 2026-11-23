"""Construye pipeline_secop.ipynb (sin ejecutar). Se ejecuta con tools/ejecutar_notebook.sh."""
import nbformat as nbf

nb = nbf.v4.new_notebook()
C = []
md = lambda t: C.append(nbf.v4.new_markdown_cell(t.strip()))
code = lambda t: C.append(nbf.v4.new_code_cell(t.strip()))

md("""
# Pipeline de datos masivos: contratación pública SECOP II, de la fuente al tablero
**Gestión de Datos Masivos G004 · Unidad 3 · I.U. Pascual Bravo** · Notebook de las **fases 2 a 8**, ejecutado de principio a fin sobre el stack Docker (Kafka + HDFS + PySpark).
Cada fase importa el módulo real de `pipeline/` (el mismo que usa `run_pipeline`) y explica en celdas de texto qué se decidió y por qué.

## Fase 1 · Fuente, pregunta y arquitectura
| | |
|---|---|
| **Fuente** | datos.gov.co (Socrata) · *SECOP II – Contratos Electrónicos* · dataset `jbjy-vk9h` (~5,7 M de registros; se actualiza a diario) |
| **Fecha** | `fecha_de_firma` |
| **id_registro** | `id_contrato` (único; verificado con `count(*) = count(distinct id_contrato)`) |
| **Pregunta** | ¿Cuánta contratación pública se adjudica de forma directa y en qué departamentos, modalidades y entidades se concentra el dinero? |
| **Usuarios** | veedurías ciudadanas, periodistas de datos y entes de control |
| **Arquitectura** | **Lambda**: capa batch (raw → curada → indicadores) + capa rápida (Structured Streaming) que se unen en el tablero |

```
API Socrata → [RAW JSONL por fecha_ingesta] → Kafka (3 part., key=id) ─┬→ consumidor → HDFS raw ─→ MapReduce ⇄ Spark
                                                                       │                      └→ PySpark → Parquet (curada, anio_mes) → calidad → SQLite (servicio) → tablero HTML
                                                                       └→ Structured Streaming (ventana 1 día, watermark, checkpoint) → SQLite (capa rápida)
```
**Por qué Lambda y no Kappa:** el batch permite reprocesar *todo* el historial desde el raw cuando cambian las reglas de limpieza y es la base de los indicadores auditables; la capa rápida solo cubre lo reciente con baja latencia. En Kappa todo pasaría por el flujo y la limpieza completa sería más difícil de rehacer.
""")
code("""
import os, sys, json, sqlite3, subprocess
os.chdir("/app"); sys.path.insert(0, "/app")
import pandas as pd
pd.set_option("display.max_colwidth", 120, "display.width", 200)
from pipeline import config
print("Fuente :", config.SOCRATA_URL)
print("Ventana:", config.INGEST_DESDE, "→", config.INGEST_HASTA)
print("Kafka  :", config.KAFKA_BOOTSTRAP, "| tópico", config.TOPIC, "| particiones", config.PARTITIONS)
print("HDFS   :", config.HDFS_WEB)
""")

md("""
## Fase 2 · Ingesta a la zona raw
`requests` con **paginación** (`$limit/$offset`, orden estable por `fecha_de_firma, id_contrato`) y **hasta 5 reintentos** con espera exponencial ante 429/5xx o errores de red.
Cada registro se guarda en un **sobre** `{fuente, dataset, id_registro, fecha_ingesta, payload}` con el registro original **intacto** en `payload`, en JSON Lines particionado por `fecha_ingesta=AAAA-MM-DD`.
Cada corrida deja un **manifiesto** (URL, parámetros, páginas, conteos, marca de agua).
""")
code("""
from pipeline import f2_ingesta
manifiesto = f2_ingesta.ejecutar()
pd.Series({k: v for k, v in manifiesto.items() if k != "parametros"}, name="manifiesto")
""")
code("""
from pipeline.common import archivos_raw
linea = json.loads(open(archivos_raw()[0], encoding="utf-8").readline())
print("Sobre (payload resumido a 6 campos):")
print(json.dumps({**linea, "payload": {k: linea["payload"][k] for k in list(linea["payload"])[:6]} | {"...": f"{len(linea['payload'])} campos"}}, ensure_ascii=False, indent=2))
""")

md("""
## Fase 3 · Mensajería con Kafka
Tópico `secop.contratos.raw` con **3 particiones**. El productor publica **cada línea del raw** con **key = id_registro** (un mismo contrato cae siempre en la misma partición) y es **idempotente** (`enable.idempotence`, `acks=all`). Un consumidor con **grupo** (`secop-hdfs-writer`) lee y confirma offsets *después* de escribir en HDFS.

**¿Qué pasa si el productor se ejecuta dos veces?** (1) Un *ledger* de archivos ya publicados evita republicar: la segunda corrida no envía nada. (2) Si alguien fuerza la republicación, Kafka sí duplica (es un log de eventos), pero la **zona curada deduplica por `id_registro`** y el streaming usa `dropDuplicates`; se demuestra con conteos en `evidencias/kafka_duplicados.txt`.
""")
code("""
from pipeline import f3_kafka_producer, f3_kafka_consumer
print(f3_kafka_producer.ejecutar())
""")
code("""
print("Segunda ejecución del productor (no debe publicar nada):")
print(f3_kafka_producer.ejecutar())
""")

md("""
## Fase 4 · Data lake en HDFS y MapReduce
El consumidor guarda en `/datalake/raw/<fuente>/<dataset>/fecha_ingesta=…/` **un archivo por lote de 5.000 mensajes** (no uno por mensaje). Los lotes se alinean a offsets múltiplos de 5.000, así el nombre es determinista y reescribirlo es idempotente.
""")
code("""
print(f3_kafka_consumer.ejecutar())
from pipeline.common import hdfs_client
cli = hdfs_client()
filas = [(d.replace(config.HDFS_RAW, "…"), f, cli.status(f"{d}/{f}")["length"]) for d, _, fs in cli.walk(config.HDFS_RAW) for f in fs]
pd.DataFrame(filas, columns=["carpeta", "archivo", "bytes"])
""")
md("""
**MapReduce vs Spark.** Mapper y reducer en Python (flujo de Hadoop Streaming: `mapper | sort | reducer`) leen el raw directamente de HDFS y calculan **número de contratos y valor por departamento**; Spark hace el mismo cálculo y se comparan.
Primero se ejecuta como tubería `mapper | sort | reducer` leyendo HDFS (la que usa `run_pipeline`). Además se ejecuta como **job real de Hadoop Streaming** (`hadoop-streaming-3.2.1.jar` en el contenedor del NameNode, que trae Python 3; el stack no incluye YARN, así que usa el ejecutor local de MapReduce de Hadoop) con `bash run_evidencias.sh hadoop`, y su salida en HDFS (`/datalake/mapreduce_out`) se compara con Spark.
""")
code("""
from pipeline import f4_mapreduce_vs_spark
_ = f4_mapreduce_vs_spark.ejecutar()
""")
code("""
# Job real de Hadoop Streaming (se lanza desde el host: bash run_evidencias.sh hadoop)
ruta = "evidencias/hadoop_streaming_job.txt"
print(open(ruta, encoding="utf-8").read() if os.path.exists(ruta) else "Aún no se ha ejecutado: bash run_evidencias.sh hadoop")
print(open("evidencias/mapreduce_vs_spark_hadoop.txt", encoding="utf-8").read().splitlines()[-2:] if os.path.exists("evidencias/mapreduce_vs_spark_hadoop.txt") else "")
""")

md("""
## Fase 5 · Zona curada con PySpark
Lee de HDFS, **quita duplicados por `id_registro`**, corrige tipos, nulos y formatos, y guarda **Parquet particionado por `anio_mes`**. Cada decisión se mide (filas antes/después) y queda en `curada_stats.json`.
**Partición `anio_mes`:** las consultas son temporales; el mes da pocas particiones grandes (el día generaría archivos diminutos) y crece de forma predecible; `departamento` está muy sesgado hacia Bogotá/Valle/Antioquia.
""")
code("""
from pipeline import f5_curada
st = f5_curada.ejecutar()
pd.DataFrame([{"problema": k, "qué se decidió": v["decision"], "por qué": v["por_que"],
               **{a: b for a, b in v.items() if a not in ("problema", "decision", "por_que")}} for k, v in st["decisiones"].items()]).set_index("problema").T
""")
code("""
t = st["tamano"]
print(f"JSONL raw en HDFS : {t['jsonl_raw_bytes']/1e6:8.1f} MB")
print(f"Parquet curada    : {t['parquet_bytes']/1e6:8.1f} MB   (−{t['reduccion_pct']} %)")
print("Descartes por razón:", st["descartes_por_razon"])
print("Partición:", st["particion"]["columna"], "→", st["particion"]["particiones"], "particiones")
""")

md("""
## Fase 6 · Calidad, carga incremental e idempotencia
**Validaciones** (4 críticas + 1 advertencia): conteo > 0, sin nulos en id/fecha/valor, sin `id_registro` repetidos, valor en [0, 1e13] y % de descartados < 5 %. Se guarda `reporte_calidad.json` con los registros descartados. **Si una crítica falla, el proceso termina con `exit 1` y `run_pipeline` se detiene.**
""")
code("""
from pipeline import f6_calidad
rep = f6_calidad.ejecutar()
pd.DataFrame(rep["validaciones"])
""")
md("""
**Carga incremental con marca de agua.** `data/state/watermark.json` guarda la última `fecha_de_firma` cargada (y los ids exactamente en esa marca, para no repetir el borde). Ampliamos la ventana hasta el 8-nov: solo debe traer lo nuevo; y repetirla debe traer 0.
""")
code("""
print("Marca de agua actual:", json.load(open("data/state/watermark.json"))["fecha_de_firma"])
config.INGEST_HASTA = "2026-11-08T00:00:00"
m1 = f2_ingesta.ejecutar()
m2 = f2_ingesta.ejecutar()
pd.DataFrame([{k: m[k] for k in ("registros_descargados", "registros_omitidos_por_marca", "registros_escritos", "marca_agua_antes", "marca_agua_despues")} for m in (m1, m2)],
             index=["ampliar ventana (solo lo nuevo)", "misma ventana otra vez (0 nuevos)"])
""")
md("""
**Idempotencia (demostración):** se corre el pipeline completo **dos veces** y se comparan los conteos de cada zona (raw, Kafka, HDFS, curada, servicio, streaming). Si alguno cambia, el resultado es `FALLÓ`. La curada se *reconstruye* desde el raw (`overwrite` de particiones) y el servicio usa `replace`, por eso repetir no duplica.
""")
code("""
from pipeline import demo_idempotencia
try:
    demo_idempotencia.main()
except SystemExit as e:
    print("código de salida:", e.code)
""")

md("""
## Fase 7 · Streaming: capa rápida (Lambda)
Spark **Structured Streaming** lee el tópico de Kafka y calcula **contratos y valor por ventana de 1 día** (event time = `fecha_de_firma`) con **watermark de 7 días** y **checkpoint**. `dropDuplicates(id_registro, event_time)` evita contar un mensaje republicado. La salida se *upserta* en SQLite (clave = ventana). Usa `Trigger.availableNow`: procesa lo pendiente y termina, lo que permite **reiniciar** y comprobar que no reprocesa ni duplica.

**Unión con el batch (Lambda):** el tablero usa la vista batch (curada, limpieza completa) y completa con la capa rápida los días posteriores al último día del batch. Cuando el batch se actualiza, la vista batch prevalece.
""")
code("""
from pipeline import demo_streaming
try:
    demo_streaming.main()
except SystemExit as e:
    print("código de salida:", e.code)
""")
code("""
from pipeline.conteos import servicio
con = sqlite3.connect(config.SERVICIO_DB)
batch = pd.read_sql("SELECT fecha_firma AS fecha, contratos AS batch_contratos, valor_total AS batch_valor FROM ind_serie_diaria", con)
rapida = pd.read_sql("SELECT ventana_inicio AS fecha, contratos AS rapida_contratos, valor_total AS rapida_valor FROM rapida_ventana_diaria", con)
cmp = batch.merge(rapida, on="fecha", how="outer"); cmp["coinciden"] = (cmp.batch_contratos == cmp.rapida_contratos)
print("Capa batch vs capa rápida, por día:"); cmp
""")

md("""
## Fase 8 · Zona de servicio, tablero y repositorio
`f8_servicio` carga en **SQLite** los indicadores ya calculados, para que el tablero no haga cálculos: `ind_directa_por_departamento`, `ind_modalidad`, `ind_top_entidades`, `ind_serie_diaria` (+ `ind_cubo_dia_dep_modalidad`, `ind_entidades` y `meta_corte` con fuente y fecha de corte).
El tablero es **Plotly HTML** (una de las herramientas permitidas) y se publica en GitHub Pages: burbujas animadas (D3) que se mueven entre nube, grupos por departamento y dispersión, mapa de municipios sobre OpenStreetMap (coordenadas DIVIPOLA), mapa coroplético, barras apiladas, treemap, serie diaria con eje secundario, mapa de calor y tabla con búsqueda; con filtros por fechas, departamento y modalidad.
""")
code("""
con = sqlite3.connect(config.SERVICIO_DB)
for t in ["ind_directa_por_departamento", "ind_modalidad", "ind_top_entidades", "ind_serie_diaria", "meta_corte"]:
    n = con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
    print(f"--- {t}: {n} filas"); display(pd.read_sql(f"SELECT * FROM {t} LIMIT 4", con))
""")
code("""
from dashboard import generar
generar.main()
""")
md("""
### Resultado
![Tablero](evidencias/tablero_completo.png)

**Publicado en GitHub Pages:** ver el enlace en el `README.md`. **Evidencias** en `evidencias/` (Kafka, HDFS `ls`, MapReduce vs Spark, idempotencia, streaming tras reinicio, calidad).
""")
nb.cells = C
nb.metadata["kernelspec"] = {"name": "python3", "display_name": "Python 3", "language": "python"}
nbf.write(nb, "pipeline_secop.ipynb")
print("notebook creado con", len(C), "celdas")
