#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os, re, hashlib, pandas as pd

IN_DIR = "docs/llm_resultados"
OUT_MAESTRO = "docs/analisis/master_respuestas.csv"
OUT_IDX = "docs/analisis/index_archivos.csv"

os.makedirs("docs/analisis", exist_ok=True)

def hash_texto(s: str) -> str:
    return hashlib.sha256((s or "").encode("utf-8")).hexdigest()[:16]

def normaliza_q(q: str) -> str:
    q = (q or "").strip().lower()
    q = re.sub(r"\s+", " ", q)
    return q

def main():
    filas = []
    index_rows = []
    for fn in sorted(os.listdir(IN_DIR)):
        if not fn.endswith(".csv") or fn.endswith("_datos_sql.csv"):
            continue
        ruta = os.path.join(IN_DIR, fn)
        try:
            df = pd.read_csv(ruta)
        except Exception:
            continue
        if not {"pregunta","respuesta","fragmentos"}.issubset(df.columns):
            continue

        q = df.loc[0,"pregunta"]
        resp = df.loc[0,"respuesta"]
        frags = df.loc[0,"fragmentos"]

        q_norm = normaliza_q(q)
        h = hash_texto(q_norm + "\n" + (resp or ""))

        # Busca si existe archivo de datos SQL asociado
        base = fn[:-4]
        ruta_sql = os.path.join(IN_DIR, base + "_datos_sql.csv")
        tiene_sql = os.path.exists(ruta_sql)

        filas.append({
            "archivo": fn,
            "pregunta": q,
            "pregunta_norm": q_norm,
            "respuesta": resp,
            "fragmentos": frags,
            "hash_respuesta": h,
            "tiene_datos_sql": int(tiene_sql),
            "ruta_datos_sql": ruta_sql if tiene_sql else ""
        })
        index_rows.append({"archivo": fn, "ruta": ruta})

    maestro = pd.DataFrame(filas)
    # Deduplicado: misma pregunta_norm + misma respuesta (hash)
    antes = len(maestro)
    maestro = maestro.drop_duplicates(subset=["pregunta_norm","hash_respuesta"], keep="first")
    despues = len(maestro)

    pd.DataFrame(index_rows).to_csv(OUT_IDX, index=False)
    maestro.to_csv(OUT_MAESTRO, index=False)
    print(f"[OK] Maestro guardado: {OUT_MAESTRO} (deduplicados {antes - despues})")
    print(f"[OK] Índice de archivos: {OUT_IDX}")

if __name__ == "__main__":
    main()
