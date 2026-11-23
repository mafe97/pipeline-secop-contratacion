"""Genera docs/index.html (tablero Plotly HTML) leyendo SOLO la zona de servicio (SQLite).

El HTML es estático (apto para GitHub Pages): los datos de los indicadores se incrustan como JSON y
los filtros del tablero solo suman/filtran esas tablas ya agregadas.
    python -m dashboard.generar
"""
import json
import os
import shutil
import sqlite3
import unicodedata
from pathlib import Path

import pandas as pd

BASE = Path(__file__).parent
DB = Path(os.getenv("SERVICIO_DB", Path(os.getenv("DATA_DIR", "data")) / "servicio" / "secop.db"))
SALIDA = Path(os.getenv("DASHBOARD_OUT", "docs/index.html"))
REPO_URL = os.getenv("REPO_URL", "")

ESPECIALES = {"distrito capital de bogota": "BOGOTA, D.C.",
              "san andres, providencia y santa catalina": "ARCHIPIELAGO DE SAN ANDRES, PROVIDENCIA Y SANTA CATALINA"}


def norm(t: str) -> str:
    t = unicodedata.normalize("NFD", str(t)).encode("ascii", "ignore").decode().upper()
    return " ".join(t.split())


def clave_geo(dep: str) -> str | None:
    k = norm(dep)
    return ESPECIALES.get(k.lower(), k)


def main() -> None:
    con = sqlite3.connect(DB)
    q = lambda t: pd.read_sql(f"SELECT * FROM {t}", con)
    meta = q("meta_corte").iloc[0].to_dict()
    cubo, ent = q("ind_cubo_dia_dep_modalidad"), q("ind_entidades")
    try:
        rap = q("rapida_ventana_diaria")
    except Exception:
        rap = pd.DataFrame(columns=["ventana_inicio", "contratos", "valor_total"])
    con.close()

    geo = json.loads((BASE / "assets" / "colombia_departamentos.geojson").read_text(encoding="utf-8"))
    for f in geo["features"]:  # clave sin tildes para unir con el nombre de SECOP
        f["properties"] = {"clave": norm(f["properties"]["DPTO_CNMBR"]), "nombre": f["properties"]["DPTO_CNMBR"].title()}
    deps = sorted(cubo["departamento"].unique())
    datos = {
        "meta": {k: str(v) for k, v in meta.items()},
        "repo": REPO_URL,
        "cubo": cubo.round(0).values.tolist(),          # [fecha, departamento, modalidad, contratos, valor]
        "geoClave": {d: clave_geo(d) for d in deps},
        "entidades": ent.to_dict("records"),
        "rapida": rap.rename(columns={"ventana_inicio": "fecha"}).to_dict("records"),
    }
    html = (BASE / "plantilla.html").read_text(encoding="utf-8")
    html = html.replace("/*__DATOS__*/null", json.dumps(datos, ensure_ascii=False))
    html = html.replace("/*__GEO__*/null", json.dumps(geo, ensure_ascii=False, separators=(",", ":")))
    SALIDA.parent.mkdir(parents=True, exist_ok=True)
    SALIDA.write_text(html, encoding="utf-8")
    shutil.copytree(BASE / "vendor", SALIDA.parent / "vendor", dirs_exist_ok=True)  # librerías locales: funciona sin internet
    print(f"tablero generado: {SALIDA} ({SALIDA.stat().st_size / 1e3:.0f} KB) · corte {meta['fecha_corte']}")


if __name__ == "__main__":
    main()
