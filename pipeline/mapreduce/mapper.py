#!/usr/bin/env python3
"""Mapper (Hadoop Streaming): lee sobres JSONL por stdin y emite  departamento<TAB>1<TAB>valor.
Compatible con Python 3.5 (el que trae la imagen de Hadoop): sin f-strings."""
import json
import sys

for linea in sys.stdin:
    linea = linea.strip()
    if not linea:
        continue
    try:
        p = json.loads(linea)["payload"]
    except (ValueError, KeyError):
        continue
    dep = (p.get("departamento") or "").strip() or "SIN DATO"
    try:
        valor = float(p.get("valor_del_contrato"))
    except (TypeError, ValueError):
        valor = 0.0
    print("{}\t1\t{!r}".format(dep, valor))
