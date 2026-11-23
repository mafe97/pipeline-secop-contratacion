#!/usr/bin/env bash
# Construye y EJECUTA el notebook dentro del contenedor (guarda las salidas en el .ipynb).
set -euo pipefail
docker compose exec -T pipeline bash -c "python tools/build_notebook.py && jupyter nbconvert --to notebook --execute --inplace --ExecutePreprocessor.timeout=3600 pipeline_secop.ipynb"
