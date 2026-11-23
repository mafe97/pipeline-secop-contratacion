#!/usr/bin/env python3
"""Reducer: la entrada llega ordenada por clave; suma conteo y valor por departamento."""
import sys

actual, n, total = None, 0, 0.0
for linea in sys.stdin:
    clave, c, v = linea.rstrip("\n").split("\t")
    if clave != actual:
        if actual is not None:
            print(f"{actual}\t{n}\t{total!r}")
        actual, n, total = clave, 0, 0.0
    n += int(c)
    total += float(v)
if actual is not None:
    print(f"{actual}\t{n}\t{total!r}")
