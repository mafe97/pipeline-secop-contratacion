"""Fase 2 - Ingesta a la zona raw.

API Socrata (datos.gov.co) con paginación y reintentos -> sobres JSON Lines particionados por
fecha_ingesta=AAAA-MM-DD + manifiesto por corrida. Carga incremental con marca de agua
(última fecha_de_firma cargada): una segunda corrida solo trae lo nuevo.
"""
import json
import time
import uuid
from datetime import datetime, timezone

import requests

from pipeline import config
from pipeline.common import (ahora_iso, construir_sobre, escribir_json, get_logger, leer_json,
                             raw_base)

log = get_logger("ingesta")
WATERMARK = config.STATE_DIR / "watermark.json"


def pedir(sesion: requests.Session, params: dict) -> list[dict]:
    """GET con reintentos y espera exponencial ante 429/5xx/errores de red."""
    headers = {"User-Agent": "pipeline-secop-educativo/1.0"}
    if config.SOCRATA_APP_TOKEN:
        headers["X-App-Token"] = config.SOCRATA_APP_TOKEN
    ultimo = None
    for intento in range(1, config.HTTP_REINTENTOS + 1):
        try:
            r = sesion.get(config.SOCRATA_URL, params=params, headers=headers,
                           timeout=config.HTTP_TIMEOUT)
            if r.status_code in (429, 500, 502, 503, 504):
                raise requests.HTTPError(f"HTTP {r.status_code}")
            r.raise_for_status()
            datos = r.json()
            if not isinstance(datos, list):
                raise ValueError(f"respuesta inesperada: {str(datos)[:200]}")
            return datos
        except (requests.RequestException, ValueError) as e:
            ultimo = e
            espera = 2 ** intento
            log.warning("intento %s/%s falló (%s); reintento en %ss", intento,
                        config.HTTP_REINTENTOS, e, espera)
            time.sleep(espera)
    raise RuntimeError(f"la API falló tras {config.HTTP_REINTENTOS} intentos: {ultimo}")


def limite_inferior(marca: dict) -> str:
    """El mayor entre INGEST_DESDE y la marca de agua guardada."""
    wm = marca.get("fecha_de_firma")
    return max(config.INGEST_DESDE, wm) if wm else config.INGEST_DESDE


def ejecutar() -> dict:
    t0 = time.time()
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + "-" + uuid.uuid4().hex[:6]
    fecha_ingesta = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    marca = leer_json(WATERMARK, {})
    desde = limite_inferior(marca)
    ids_en_marca = set(marca.get("ids_en_marca", []))

    where = (f"{config.COL_FECHA} >= '{desde}' AND {config.COL_FECHA} < '{config.INGEST_HASTA}'")
    base = {"$where": where, "$order": f"{config.COL_FECHA},{config.COL_ID}", "$limit": config.PAGE_SIZE}
    log.info("ventana: %s", where)

    sesion = requests.Session()
    nuevos, offset, paginas, omitidos = [], 0, 0, 0
    while True:
        pagina = pedir(sesion, {**base, "$offset": offset})
        paginas += 1
        for reg in pagina:
            # >= desde incluye el borde: se omiten los ids ya cargados en esa marca exacta
            if marca.get("fecha_de_firma") == reg.get(config.COL_FECHA) and reg[config.COL_ID] in ids_en_marca:
                omitidos += 1
                continue
            nuevos.append(reg)
        log.info("página %s: %s registros (acumulado %s)", paginas, len(pagina), len(nuevos))
        if len(pagina) < config.PAGE_SIZE or (config.MAX_REGISTROS and len(nuevos) >= config.MAX_REGISTROS):
            break
        offset += config.PAGE_SIZE
    if config.MAX_REGISTROS:
        nuevos = nuevos[: config.MAX_REGISTROS]

    archivo = None
    if nuevos:
        destino = raw_base() / f"fecha_ingesta={fecha_ingesta}"
        destino.mkdir(parents=True, exist_ok=True)
        archivo = destino / f"part-{run_id}.jsonl"
        with archivo.open("w", encoding="utf-8") as f:
            for reg in nuevos:
                f.write(json.dumps(construir_sobre(reg, fecha_ingesta), ensure_ascii=False) + "\n")
        nueva_max = max(r[config.COL_FECHA] for r in nuevos)
        ids = {r[config.COL_ID] for r in nuevos if r[config.COL_FECHA] == nueva_max}
        if marca.get("fecha_de_firma") == nueva_max:
            ids |= ids_en_marca
        escribir_json(WATERMARK, {"fecha_de_firma": nueva_max, "ids_en_marca": sorted(ids),
                                  "actualizado": ahora_iso()})
    manifiesto = {
        "run_id": run_id, "fuente": config.FUENTE, "dataset": config.DATASET,
        "url": config.SOCRATA_URL, "parametros": {**base, "$offset": "0..N (paginado)"},
        "fecha_ingesta": fecha_ingesta, "paginas": paginas,
        "registros_descargados": len(nuevos) + omitidos, "registros_omitidos_por_marca": omitidos,
        "registros_escritos": len(nuevos), "archivo": str(archivo) if archivo else None,
        "marca_agua_antes": marca.get("fecha_de_firma"),
        "marca_agua_despues": leer_json(WATERMARK, {}).get("fecha_de_firma"),
        "segundos": round(time.time() - t0, 1), "finalizado": ahora_iso(),
    }
    escribir_json(raw_base() / "_manifiestos" / f"{run_id}.json", manifiesto)
    log.info("manifiesto: %s", json.dumps(manifiesto, ensure_ascii=False))
    return manifiesto


if __name__ == "__main__":
    ejecutar()
