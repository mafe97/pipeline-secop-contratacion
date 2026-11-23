"""Demostración: el flujo se detiene y se reinicia; con el checkpoint no reprocesa ni duplica."""
import subprocess
import sys

from pipeline import config
from pipeline.conteos import servicio


def correr() -> str:
    r = subprocess.run([sys.executable, "-m", "pipeline.f7_streaming"], capture_output=True, text=True)
    print(r.stdout[-1500:])
    if r.returncode:
        print(r.stderr[-3000:])
        raise SystemExit(r.returncode)
    return r.stdout


def main() -> None:
    out = []
    for i in (1, 2):
        correr()
        s = servicio()
        out.append(f"Tras arranque #{i}: ventanas={s.get('rapida_ventanas')} contratos={s.get('rapida_contratos')}")
        print(out[-1])
    ok = out[0].split(": ")[1] == out[1].split(": ")[1]
    out.append("RESULTADO: " + ("el reinicio no reprocesó ni duplicó" if ok else "CAMBIÓ tras el reinicio"))
    print(out[-1])
    config.EVIDENCIAS_DIR.mkdir(parents=True, exist_ok=True)
    (config.EVIDENCIAS_DIR / "streaming_reinicio.txt").write_text("\n".join(out) + "\n", encoding="utf-8")
    raise SystemExit(0 if ok else 1)


if __name__ == "__main__":
    main()
