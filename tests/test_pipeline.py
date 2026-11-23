import json
import subprocess
import sys
from pathlib import Path

from pipeline import config
from pipeline.common import construir_sobre
from pipeline.f2_ingesta import limite_inferior

MR = Path(__file__).parent.parent / "pipeline" / "mapreduce"


def test_sobre_conserva_registro_intacto():
    reg = {"id_contrato": "CO1.PCCNTR.1", "valor_del_contrato": "100", "extra": "x"}
    s = construir_sobre(reg, "2026-11-01")
    assert s["id_registro"] == "CO1.PCCNTR.1" and s["payload"] == reg
    assert set(s) == {"fuente", "dataset", "id_registro", "fecha_ingesta", "payload"}


def test_marca_de_agua_nunca_baja_del_desde():
    assert limite_inferior({}) == config.INGEST_DESDE
    assert limite_inferior({"fecha_de_firma": "2099-01-01T00:00:00.000"}) == "2099-01-01T00:00:00.000"


def test_mapreduce_cuenta_y_suma():
    lineas = [json.dumps({"payload": {"departamento": d, "valor_del_contrato": v}}) for d, v in
              [("Antioquia", "10"), ("Antioquia", "5"), ("Meta", "7"), (" ", "x")]]
    m = subprocess.run([sys.executable, str(MR / "mapper.py")], input="\n".join(lineas), capture_output=True, text=True)
    ordenado = "\n".join(sorted(m.stdout.splitlines())) + "\n"
    r = subprocess.run([sys.executable, str(MR / "reducer.py")], input=ordenado, capture_output=True, text=True)
    res = {l.split("\t")[0]: (int(l.split("\t")[1]), float(l.split("\t")[2])) for l in r.stdout.splitlines()}
    assert res == {"Antioquia": (2, 15.0), "Meta": (1, 7.0), "SIN DATO": (1, 0.0)}
