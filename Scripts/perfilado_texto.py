#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Perfilado sobre CSV de texto (CoWeSe) ANTES de cargar a la BD.
Lee: Textos/cowese_matches.csv
"""

import re, unicodedata, pandas as pd, os, datetime

def strip_accents(s: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFKD", str(s)) if not unicodedata.combining(c))

def norm_header(h: str) -> str:
    s = strip_accents(h).lower().strip()
    s = re.sub(r"[^\w]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s

def read_csv_smart(path: str) -> pd.DataFrame:
    for enc in ("utf-8", "latin-1"):
        try:
            df = pd.read_csv(path, sep=None, engine="python", encoding=enc)
            break
        except Exception:
            continue
    else:
        df = pd.read_csv(path, encoding="latin-1")
    df.columns = [norm_header(c) for c in df.columns]
    return df

def rename_first_match(df: pd.DataFrame, candidates, new_name) -> bool:
    for cand in candidates:
        c = norm_header(cand)
        if c in df.columns:
            if c != new_name:
                df.rename(columns={c: new_name}, inplace=True)
            return True
    return False

IN_TEXT = "Textos/cowese_matches.csv"
OUTDIR_TEXTO = "docs/Perfilado/texto"
CIE_PATTERN = re.compile(r"^F1[0-9](\..+)?$", re.IGNORECASE)

def main_texto():
    os.makedirs(OUTDIR_TEXTO, exist_ok=True)

    t = read_csv_smart(IN_TEXT)

    has_sentence = rename_first_match(
        t,
        ["sentence", "frase", "oracion", "oración", "texto", "line", "sent"],
        "sentence"
    )
    has_cie = rename_first_match(
        t,
        ["cie10_code", "cie10", "codigo_cie10", "codigo", "cod_cie10", "dx_cie10", "diagnostico_cie10", "diagnostico", "dx"],
        "cie10_code"
    )

    if not has_sentence:
        t["sentence"] = pd.NA
    if not has_cie:
        t["cie10_code"] = pd.NA

    t["sentence"] = t["sentence"].astype(str)
    t["cie10_code"] = t["cie10_code"].astype(str).str.upper().str.strip()

    sent_empty = (t["sentence"].isna()) | (t["sentence"].astype(str).str.strip() == "")
    df_conteos = pd.DataFrame({
        "total_filas":[len(t)],
        "frases_vacias":[int(sent_empty.sum())]
    })
    df_conteos.to_csv(os.path.join(OUTDIR_TEXTO, "01_conteos_frases.csv"), index=False, encoding="utf-8")

    dup = t.duplicated(subset=["sentence","cie10_code"], keep=False)
    df_dups = pd.DataFrame({"filas_en_grupos_duplicados":[int(dup.sum())]})
    df_dups.to_csv(os.path.join(OUTDIR_TEXTO, "02_duplicados.csv"), index=False, encoding="utf-8")

    fuera = ~(t["cie10_code"].fillna("").str.match(CIE_PATTERN))
    df_fuera = pd.DataFrame({"fuera_de_rango":[int(fuera.sum())]})
    df_fuera.to_csv(os.path.join(OUTDIR_TEXTO, "03_fuera_rango.csv"), index=False, encoding="utf-8")

    md = os.path.join(OUTDIR_TEXTO, "perfilado_csv_texto_resumen.md")
    with open(md, "w", encoding="utf-8") as f:
        f.write("# Perfilado CSV – Texto (CoWeSe)\n\n")
        f.write(f"_Generado: {datetime.datetime.now().isoformat(timespec='seconds')}_\n\n")
        f.write("- 01_conteos_frases.csv\n")
        f.write("- 02_duplicados.csv\n")
        f.write("- 03_fuera_rango.csv\n\n")
        f.write("## Diagnóstico rápido\n")
        f.write(f"- Columnas detectadas: {list(t.columns)}\n")
        f.write(f"- ¿Tenía 'sentence' original?: {has_sentence}\n")
        f.write(f"- ¿Tenía 'cie10_code' original?: {has_cie}\n")

    print("[OK] CSV perfilado (Texto) →", OUTDIR_TEXTO)
    print("[INFO] TEXTO cols:", list(t.columns))

if __name__ == "__main__":
    main_texto()
