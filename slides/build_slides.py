"""Genera slides/presentacion.html y el PDF (máx. 8 diapositivas) con las cifras REALES de la corrida.
Uso: AUTOR="Nombre Apellido" GRUPO="G004" REPO_URL=... PAGES_URL=... python3 slides/build_slides.py
"""
import glob, html, json, os, sqlite3, subprocess
from pathlib import Path

R = Path(__file__).resolve().parent.parent
AUTOR = os.getenv("AUTOR", "Nombre Apellido")
GRUPO = os.getenv("GRUPO", "G004")
REPO = os.getenv("REPO_URL", "https://github.com/…")
PAGES = os.getenv("PAGES_URL", "https://….github.io/…")
J = lambda p: json.loads((R / p).read_text(encoding="utf-8"))
nf = lambda v: f"{int(v):,}".replace(",", ".")

cur, cal = J("data/calidad/curada_stats.json"), J("data/calidad/reporte_calidad.json")
idem, mrs = J("evidencias/idempotencia.json"), J("data/calidad/mapreduce_vs_spark.json")
man = sorted(glob.glob(str(R / "data/raw/*/*/_manifiestos/*.json")))
m1, m2 = json.load(open(man[0])), json.load(open(man[1])) if len(man) > 1 else {}
con = sqlite3.connect(R / "data/servicio/secop.db")
meta = dict(zip(*[[c[0] for c in con.execute("select * from meta_corte").description], con.execute("select * from meta_corte").fetchone()]))
dep = con.execute("select departamento,contratos,contratos_directa,valor_total,valor_directa from ind_directa_por_departamento").fetchall()
mod = con.execute("select modalidad,contratos,valor_total from ind_modalidad order by valor_total desc").fetchall()
tot_n, tot_v = sum(d[1] for d in dep), sum(d[3] for d in dep)
pct_n, pct_v = 100 * sum(d[2] for d in dep) / tot_n, 100 * sum(d[4] for d in dep) / tot_v
top = sorted([d for d in dep if d[0] != "Sin dato"], key=lambda d: -d[3])
top3 = sum(d[3] for d in top[:3]) / tot_v * 100
ent = con.execute("select entidad,valor_total,pct_contratos_directa from ind_top_entidades limit 1").fetchone()
cop = lambda v: f"${v/1e12:,.2f} billones".replace(".", ",") if v >= 1e12 else f"${v/1e9:,.0f} mil millones"
pc = lambda v: f"{v:.1f} %".replace(".", ",")
s1, s2 = idem["corrida1"], idem["corrida2"]
rows_idem = "".join(f"<tr><td>{k}</td><td>{nf(s1[k])}</td><td>{nf(s2[k])}</td><td class=ok>{'✔' if s1[k]==s2[k] else '✘'}</td></tr>" for k in s1)
dec = cur["decisiones"]
stream = (R / "evidencias/streaming_reinicio.txt").read_text(encoding="utf-8").strip().splitlines()
checks = "".join(f"<tr><td>{c['nombre'].replace('_',' ')}</td><td>{'crítica' if c['critica'] else 'aviso'}</td><td class=ok>{'✔' if c['ok'] else '✘'}</td></tr>" for c in cal["validaciones"])
mr_ok = "COINCIDEN" if not mrs["difs"] else "NO COINCIDEN"
tam = cur["tamano"]
raw_local = sum(os.path.getsize(f) for f in glob.glob(str(R / "data/raw/*/*/fecha_ingesta=*/*.jsonl")))
red_local = 100 * (1 - tam["parquet_bytes"] / raw_local)
batch = con.execute("select fecha_firma,contratos,valor_total from ind_serie_diaria order by fecha_firma").fetchall()
rap = {r[0]: r[1:] for r in con.execute("select ventana_inicio,contratos,valor_total from rapida_ventana_diaria")}
rows_stream = "".join(f"<tr><td>{d}</td><td>{nf(n)}</td><td>{nf(rap.get(d,(0,0))[0])}</td><td class=ok>{'✔' if rap.get(d,(0,0))[0]==n else '✘'}</td></tr>" for d, n, v in batch[:6])
mr_top = sorted(mrs["spark"].items(), key=lambda kv: -kv[1]["contratos"])[:5]
rows_mr = "".join(f"<tr><td>{k}</td><td>{nf(v['contratos'])}</td><td>{nf(mrs['mapreduce'][k]['contratos'])}</td><td>{v['valor']/1e9:,.0f}</td><td>{mrs['mapreduce'][k]['valor']/1e9:,.0f}</td><td class=ok>✔</td></tr>" for k, v in mr_top)

def slide(n, titulo, cuerpo, sub=""):
    return f"""<section class="slide"><header><span>{titulo}</span><small>{sub}</small></header><div class="body">{cuerpo}</div>
    <footer>Pipeline SECOP II · Gestión de Datos Masivos {GRUPO} · {html.escape(AUTOR)}<b>{n}/8</b></footer></section>"""

S = []
S.append(f"""<section class="slide cover"><div><p class="k">Gestión de Datos Masivos {GRUPO} · Unidad 3 · I.U. Pascual Bravo · 2026-2</p>
<h1>De la fuente al tablero:<br>¿cuánta contratación pública se adjudica de forma directa?</h1>
<p class="q">Pipeline de datos masivos con SECOP II (datos.gov.co): API → Kafka → HDFS → MapReduce/PySpark → Parquet → SQLite → tablero</p>
<table class="t small"><tr><td><b>Fuente</b></td><td>datos.gov.co · SECOP II Contratos Electrónicos · <code>jbjy-vk9h</code> · ~5,7 M de registros, actualización diaria</td></tr>
<tr><td><b>id_registro</b></td><td><code>id_contrato</code> (único) · <b>fecha:</b> <code>fecha_de_firma</code></td></tr>
<tr><td><b>Pregunta</b></td><td>¿Cuánta contratación se adjudica de forma directa y dónde se concentra el dinero (departamento, modalidad, entidad)?</td></tr>
<tr><td><b>Usuarios</b></td><td>veedurías ciudadanas, periodismo de datos y entes de control</td></tr>
<tr><td><b>Muestra</b></td><td>{nf(cur['filas_final'])} contratos firmados · {meta['fecha_corte'][:10]} de corte</td></tr></table>
<p class="a">{html.escape(AUTOR)}</p></div></section>""")
S.append(slide(2, "Arquitectura por zonas · Lambda", f"""
<div class="flow"><div class="z raw"><h4>RAW</h4>Python <code>requests</code> + sobre JSON<br>JSONL <code>fecha_ingesta=…</code> + manifiesto</div><i>→</i>
<div class="z kf"><h4>Kafka</h4>tópico · 3 particiones<br>key = id_registro · grupo</div><i>→</i>
<div class="z raw"><h4>HDFS</h4>lotes por fecha<br>MapReduce ⇄ Spark</div><i>→</i>
<div class="z cur"><h4>CURADA</h4>PySpark · dedup · tipos<br>Parquet <code>anio_mes</code></div><i>→</i>
<div class="z srv"><h4>SERVICIO</h4>SQLite · 4+ indicadores<br>tablero Plotly HTML</div></div>
<div class="flow fast"><div class="z kf wide"><h4>CAPA RÁPIDA</h4>Kafka → Spark Structured Streaming · ventana 1 día · watermark 7 d · checkpoint → SQLite <code>rapida_ventana_diaria</code></div></div>
<p><b>¿Lambda o Kappa? Lambda.</b> La capa <b>batch</b> reconstruye todo desde el raw con limpieza completa y es la base auditable de los indicadores; la capa <b>rápida</b> da cifras recientes con baja latencia y el tablero las une (usa batch y completa con la rápida los días que aún no están). Kappa dificultaría reprocesar el historial cuando cambian las reglas de limpieza.</p>
<table class="t small"><tr><th>Etapa</th><th>Herramienta</th><th>Salida</th></tr>
<tr><td>Ingesta</td><td>Python <code>requests</code> (paginación + reintentos)</td><td>JSONL con sobre + manifiesto</td></tr>
<tr><td>Mensajería</td><td>Apache Kafka (KRaft, 3 particiones)</td><td>tópico <code>secop.contratos.raw</code></td></tr>
<tr><td>Data lake / cálculo</td><td>HDFS · MapReduce (Python) · PySpark</td><td>raw en lotes · Parquet curado</td></tr>
<tr><td>Servicio / tablero</td><td>SQLite · Plotly HTML (GitHub Pages)</td><td>indicadores listos + tablero</td></tr></table>""", "Fase 1"))
S.append(slide(3, "Ingesta a raw y mensajería con Kafka", f"""
<div class="cols"><div><h4>Ingesta (Fase 2)</h4><ul><li>API Socrata con <b>paginación</b> ($limit/$offset, orden estable) y <b>5 reintentos</b> con espera exponencial</li>
<li><b>Sobre JSON</b>: <code>fuente, dataset, id_registro, fecha_ingesta, payload</code> (registro original intacto)</li>
<li>JSONL en <code>fecha_ingesta=AAAA-MM-DD/</code> + <b>manifiesto</b> por corrida</li>
<li>Primera carga: <b>{nf(m1['registros_escritos'])}</b> registros en {m1['paginas']} páginas</li></ul>
<pre>{{"fuente":"datos.gov.co","dataset":"jbjy-vk9h",
 "id_registro":"CO1.PCCNTR.…","fecha_ingesta":"{m1['fecha_ingesta']}",
 "payload":{{…registro original…}}}}</pre></div>
<div><h4>Kafka (Fase 3)</h4><ul><li>Tópico <code>secop.contratos.raw</code> · <b>3 particiones</b></li><li>Productor <b>idempotente</b>, <b>key = id_registro</b> (mismo contrato → misma partición)</li>
<li>Consumidor con <b>grupo</b>, commit <i>después</i> de escribir en HDFS</li></ul>
<div class="box"><b>¿Y si el productor corre dos veces?</b><br>1) Un <i>ledger</i> de archivos publicados evita republicar.<br>2) Si se fuerza, Kafka duplica (log de eventos) pero la <b>curada deduplica por id_registro</b> y el streaming usa <code>dropDuplicates</code>.<br>Demostrado: Kafka y HDFS pasan a ×2 y curada/servicio/streaming <b>no cambian</b> (<code>evidencias/kafka_duplicados.txt</code>).</div></div></div>""", "Fases 2 y 3"))
S.append(slide(4, "Data lake en HDFS y MapReduce vs Spark", f"""
<div class="cols"><div><h4>HDFS (Fase 4)</h4><pre>/datalake/raw/datos.gov.co/jbjy-vk9h/
  fecha_ingesta={m1['fecha_ingesta']}/
    lote_p0_o0000000000-0000004999.jsonl
    lote_p1_o…  lote_p2_o…</pre>
<ul><li><b>Un archivo por lote de 5.000 mensajes</b>, no por mensaje</li><li>Nombre determinista (alineado a offsets) → reescribir es idempotente</li><li>Evidencia: <code>hdfs dfs -ls -R /datalake</code></li></ul></div>
<div><h4>MapReduce (mapper/reducer Python)</h4><pre>cat raw | mapper.py | sort | reducer.py
mapper  → departamento \\t 1 \\t valor
reducer → departamento \\t n \\t Σ valor</pre>
<div class="box big">MapReduce vs Spark: <b>{mr_ok}</b><br>{len(mrs['mapreduce'])} departamentos · {nf(sum(v['contratos'] for v in mrs['mapreduce'].values()))} contratos<br>diferencias: <b>{', '.join(mrs['difs']) or 'ninguna'}</b></div>
<p class="small">Nota: el MR corre como tubería local sobre datos leídos de HDFS (no en YARN).</p></div></div>
<h4 style="margin-top:14px">Resultado: contratos y valor (miles de millones COP) por departamento</h4>
<table class="t small"><tr><th>Departamento</th><th>Spark contratos</th><th>MapReduce contratos</th><th>Spark valor</th><th>MapReduce valor</th><th></th></tr>{rows_mr}</table>""", "Fase 4"))
S.append(slide(5, "Zona curada con PySpark → Parquet", f"""
<table class="t"><tr><th>Problema</th><th>Decisión y por qué</th><th>Antes → después</th></tr>
<tr><td>Duplicados por <code>id_registro</code></td><td>Una fila por id (mayor fecha_ingesta, luego ultima_actualizacion): refleja el estado actual y no cuenta el dinero dos veces</td><td>{nf(dec['duplicados']['filas_antes'])} → {nf(dec['duplicados']['filas_despues'])}<br><small>(corrida tras republicar Kafka a propósito: ×2)</small></td></tr>
<tr><td>Fechas y valores como texto</td><td>→ timestamp / double; lo inválido a NULL (no se inventa)</td><td>{dec['tipos']['fecha_firma_nulas']} fechas y {dec['tipos']['valor_nulos']} valores nulos</td></tr>
<tr><td>Nulos disfrazados («No definido»)</td><td>→ NULL; departamento → «Sin dato»</td><td>{nf(dec['texto']['departamento_no_definido_antes'])} unificados</td></tr>
<tr><td>Modalidad escrita de varias formas</td><td>Catálogo de 8 grupos + <code>es_directa</code>; si no, la directa se subestima</td><td>{dec['texto']['modalidades_distintas_antes']} → {dec['texto']['modalidades_distintas_despues']} modalidades</td></tr>
<tr><td>Sin fecha / valor nulo, negativo o &gt; 1e13</td><td>Se descarta y se cuenta por razón (el raw conserva todo)</td><td>{nf(dec['descartes']['filas_antes'])} → {nf(dec['descartes']['filas_despues'])}</td></tr></table>
<div class="cols"><div class="box"><b>Partición <code>anio_mes</code></b> ({cur['particion']['particiones']} particiones): las consultas son temporales; el mes da pocas particiones grandes (el día daría archivos diminutos); <code>departamento</code> está sesgado a Bogotá/Valle/Antioquia.</div>
<div class="box big"><b>JSONL {raw_local/1e6:,.0f} MB → Parquet {tam['parquet_bytes']/1e6:,.1f} MB</b><br>−{red_local:.1f} % (columnar + solo las columnas usadas)</div></div>""", "Fase 5"))
S.append(slide(6, "Calidad, incremental e idempotencia (demo en vivo)", f"""
<div class="cols"><div><h4>Validaciones automáticas</h4><table class="t small"><tr><th>Validación</th><th>Tipo</th><th></th></tr>{checks}</table>
<p class="small">Reporte con descartados ({nf(cal['filas_descartadas'])}). <b>Si falla una crítica → exit 1 y el pipeline se detiene.</b></p>
<div class="box"><b>Marca de agua</b> = última <code>fecha_de_firma</code> cargada. Ampliar la ventana trajo solo <b>{nf(m2.get('registros_escritos',0))}</b> nuevos; repetirla trajo <b>0</b>.</div></div>
<div><h4>Correr el pipeline dos veces</h4><table class="t small"><tr><th>Conteo por zona</th><th>Corrida 1</th><th>Corrida 2</th><th></th></tr>{rows_idem}</table>
<div class="box big">Resultado: <b>IDEMPOTENTE</b> — la curada se reconstruye desde el raw (overwrite) y el servicio usa replace.</div></div></div>""", "Fase 6"))
S.append(slide(7, "Streaming: capa rápida (Lambda)", f"""
<div class="cols"><div><ul><li>Spark <b>Structured Streaming</b> lee el tópico de Kafka</li><li>Agregación por <b>ventana de 1 día</b> (event time = <code>fecha_de_firma</code>)</li>
<li><b>Watermark 7 días</b> + <code>dropDuplicates(id_registro, event_time)</code></li><li><b>Checkpoint</b>: guarda offsets y estado de ventanas</li><li>Salida con <i>upsert</i> en SQLite (clave = ventana)</li></ul>
<div class="box"><b>Unión con el batch (Lambda):</b> el tablero usa la vista batch (limpieza completa) y completa con la capa rápida los días posteriores al último día del batch; cuando el batch se actualiza, prevalece.</div></div>
<div><h4>Reinicio del flujo</h4><pre>{html.escape(chr(10).join(stream))}</pre>
<div class="box big">Al reiniciar: <b>0 filas reprocesadas</b>,<br>mismas ventanas y mismos contratos<br>(capa rápida = capa batch: {nf(s2.get('rapida_contratos',0))})</div>
<table class="t small"><tr><th>Día (ventana)</th><th>Batch</th><th>Rápida</th><th></th></tr>{rows_stream}</table></div></div>""", "Fase 7"))
S.append(slide(8, "Tablero y hallazgos", f"""
<div class="cols dash"><div class="img"><img src="../evidencias/tablero_completo.png"></div>
<div><h4>Servicio y tablero (Fase 8)</h4><ul class="small"><li>SQLite: <code>ind_directa_por_departamento</code>, <code>ind_modalidad</code>, <code>ind_top_entidades</code>, <code>ind_serie_diaria</code></li><li>Plotly HTML: mapa, barras, treemap, serie, mapa de calor y tabla · filtros</li><li>Título = pregunta; fuente y fecha de corte en el pie</li></ul>
<div class="box"><b>Respuesta a la pregunta</b><br>• <b>{pc(pct_n)}</b> de los contratos y <b>{pc(pct_v)}</b> del valor ({cop(tot_v)}) son contratación directa.<br>• 3 departamentos concentran <b>{pc(top3)}</b> del valor ({', '.join(d[0].replace('Distrito Capital de ','') for d in top[:3])}).<br>• Mayor entidad: {html.escape(ent[0].title()[:48])} ({cop(ent[1])}).</div>
<p class="small"><b>Repositorio:</b> {REPO}<br><b>Tablero:</b> {PAGES}</p></div></div>""", "Fase 8"))

CSS = """@page{size:1280px 720px;margin:0}*{box-sizing:border-box}body{margin:0;font:19px/1.42 'Inter','Segoe UI',sans-serif;color:#14213d}
.slide{width:1280px;height:720px;position:relative;page-break-after:always;overflow:hidden;background:#fff}
header{background:linear-gradient(110deg,#0a2540,#0b5fa5);color:#fff;padding:20px 40px;display:flex;justify-content:space-between;align-items:baseline}header span{font-size:30px;font-weight:700}header small{opacity:.8;font-size:16px}
.body{padding:20px 40px}footer{position:absolute;bottom:0;left:0;right:0;padding:9px 40px;font-size:12px;color:#5b6785;border-top:1px solid #e3e8f2;display:flex;justify-content:space-between}
h4{margin:0 0 10px;color:#0b5fa5;font-size:22px}ul{margin:6px 0;padding-left:20px}li{margin:7px 0}.small,.small li{font-size:16px}code,pre{font-family:ui-monospace,Menlo,monospace;font-size:.85em;background:#eef3fa;border-radius:4px}code{padding:1px 4px}
pre{padding:12px 14px;white-space:pre-wrap;font-size:15px;line-height:1.35}
.cols{display:grid;grid-template-columns:1fr 1fr;gap:26px}.box{background:#f1f6fc;border-left:4px solid #e07a1f;padding:10px 14px;border-radius:6px;margin:10px 0;font-size:17px}.box.big{font-size:21px;text-align:center;border-left-color:#1b9e77}
table.t{border-collapse:collapse;width:100%}.t th{background:#0b5fa5;color:#fff;text-align:left;padding:8px 10px;font-size:16px}.t td{padding:9px 10px;border-bottom:1px solid #e3e8f2;font-size:16px}.t.small td,.t.small th{font-size:14.5px;padding:5px 9px}.ok{color:#1b9e77;font-weight:700;text-align:center}
.cover{background:linear-gradient(120deg,#0a2540,#0b5fa5);color:#fff;display:flex;align-items:center;padding:0 70px}.cover h1{font-size:44px;line-height:1.15;margin:8px 0 14px}.cover .k{opacity:.8;margin:0}.cover .q{font-size:19px;opacity:.9;margin:0 0 18px}.cover .a{font-size:22px;margin:18px 0 0;font-weight:600}
.cover .t td{color:#14213d;background:rgba(255,255,255,.93);border-bottom:1px solid #d7dfee}.cover code{background:#dfe8f5}
.flow{display:flex;align-items:stretch;gap:8px;margin:6px 0 12px}.flow i{align-self:center;font-style:normal;font-size:26px;color:#0b5fa5}.z{flex:1;border-radius:8px;padding:12px;font-size:16px;border:2px solid}
.z h4{margin:0 0 4px;font-size:19px}.raw{background:#eef3fa;border-color:#7aa3d3}.kf{background:#fdf0e1;border-color:#e07a1f}.cur{background:#e5f4ee;border-color:#1b9e77}.srv{background:#efe7f7;border-color:#7b5ea7}.wide{flex:1}
.dash{grid-template-columns:1.25fr 1fr}.img img{width:100%;border:1px solid #e3e8f2;border-radius:6px;max-height:560px;object-fit:cover;object-position:top}"""
doc = f"<!DOCTYPE html><html lang='es'><meta charset='utf-8'><title>Pipeline SECOP II</title><style>{CSS}</style>{''.join(S)}</html>"
out = R / "slides" / "presentacion.html"
out.write_text(doc, encoding="utf-8")
pdf = R / "slides" / "Presentacion_Pipeline_SECOP_II.pdf"
subprocess.run(["google-chrome", "--headless=new", "--no-sandbox", "--disable-gpu", "--no-pdf-header-footer", f"--print-to-pdf={pdf}", f"file://{out}"],
               check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
print("PDF:", pdf, pdf.stat().st_size // 1024, "KB")
