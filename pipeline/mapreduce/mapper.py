#!/usr/bin/env python3
"""Mapper (estilo Hadoop Streaming): lee sobres JSONL por stdin y emite  departamento<TAB>1<TAB>valor."""
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
    print(f"{dep}\t1\t{valor!r}")
