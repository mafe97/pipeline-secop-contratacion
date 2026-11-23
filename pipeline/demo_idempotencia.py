"""Demostración en vivo: correr el pipeline dos veces y comparar conteos en cada zona."""
import json

from pipeline import config
from pipeline.common import get_logger
from pipeline.conteos import snapshot
from pipeline.run_pipeline import ejecutar

log = get_logger("idempotencia")


def main() -> None:
    ejecutar()
    s1 = snapshot()
    log.info("conteos tras la corrida 1: %s", s1)
    ejecutar()
    s2 = snapshot()
    log.info("conteos tras la corrida 2: %s", s2)
    filas = [f"{'zona / conteo':34}{'corrida 1':>14}{'corrida 2':>14}  igual"]
    ok = True
    for k in s1:
        igual = s1[k] == s2[k]
        ok &= igual
        filas.append(f"{k:34}{s1[k]:>14}{s2[k]:>14}  {'SI' if igual else 'NO  <-- duplicó/cambió'}")
    filas.append("\nRESULTADO: " + ("IDEMPOTENTE - la segunda corrida no duplicó nada" if ok else "FALLÓ"))
    texto = "\n".join(filas)
    print(texto)
    config.EVIDENCIAS_DIR.mkdir(parents=True, exist_ok=True)
    (config.EVIDENCIAS_DIR / "idempotencia.txt").write_text(texto + "\n", encoding="utf-8")
    (config.EVIDENCIAS_DIR / "idempotencia.json").write_text(json.dumps({"corrida1": s1, "corrida2": s2}, indent=2))
    raise SystemExit(0 if ok else 1)


if __name__ == "__main__":
    main()
