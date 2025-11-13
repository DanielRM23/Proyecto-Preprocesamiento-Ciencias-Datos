#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Perfilado sobre CSV de hechos (defunciones / urgencias) ANTES de cargar a la BD.
Lee: Data/defunciones_uso_sustancias_clean.csv, Data/urgencias_uso_sustancias_clean.csv
Genera métricas en docs/Entrega4_perfilado_csv/sql/
"""

import os, re, datetime, unicodedata
import pandas as pd

IN_DEF = "Data/defunciones_uso_sustancias.csv"
IN_URG = "Data/urgencias_uso_sustancias.csv"
OUTDIR = "docs/perfilado/csv"

F_WIDE_PATTERN = re.compile(r"^f1[0-9]$")    # f10..f19 (normalizados)
CIE_PATTERN    = re.compile(r"^f1[0-9](\..+)?$", re.IGNORECASE)  # p.ej. F14 o F14.2

# ---------------- utilidades ----------------
def ensure_outdir():
    os.makedirs(OUTDIR, exist_ok=True)

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

def map_synonyms(df: pd.DataFrame, is_def: bool) -> pd.DataFrame:
    year_opts = ["anio","ano","año","year","anio_defuncion","ano_defuncion","anio_urgencia","ano_urgencia"]
    ent_opts  = ["entidad_norm","entidad","entidad_defuncion_etq","entidad_defuncion","nom_ent","estado","nombre_entidad"]
    sex_opts  = ["sexo","genero","género","sex"]
    cie_opts  = ["cie10_code","cie10","codigo_cie10","dx_cie10","diagnostico","diagnostico_cie10","dx","codigo"]
    val_opts  = ["valor","conteo","cantidad","total","n","eventos"]

    def pick(opts):
        for o in opts:
            o2 = norm_header(o)
            if o2 in df.columns:
                return o2
        return None

    mapping = {
        "anio": pick(year_opts),
        "entidad_norm": pick(ent_opts),
        "sexo": pick(sex_opts),
        "cie10_code": pick(cie_opts),
        "valor": pick(val_opts)
    }

    rename = {v:k for k,v in mapping.items() if v and v != k}
    if rename:
        df = df.rename(columns=rename)

    if is_def:
        if "entidad_defuncion_etq" in df.columns and "entidad_norm" not in df.columns:
            df = df.rename(columns={"entidad_defuncion_etq":"entidad_norm"})
        elif "entidad_defuncion" in df.columns and "entidad_norm" not in df.columns:
            df = df.rename(columns={"entidad_defuncion":"entidad_norm"})

    return df

def is_wide(df: pd.DataFrame) -> bool:
    cols = set(df.columns)
    return any(c for c in cols if F_WIDE_PATTERN.match(c))

def melt_to_long(df: pd.DataFrame) -> pd.DataFrame:
    id_cols = [c for c in df.columns if not F_WIDE_PATTERN.match(c)]
    fcols   = [c for c in df.columns if F_WIDE_PATTERN.match(c)]
    long_df = df.melt(id_vars=id_cols, value_vars=fcols, var_name="cie10_code", value_name="valor")
    long_df["cie10_code"] = long_df["cie10_code"].str.upper()
    if "valor" in long_df.columns:
        long_df["valor"] = pd.to_numeric(long_df["valor"], errors="coerce")
        long_df = long_df[long_df["valor"].notna()]
    return long_df

def ensure_long_schema(df: pd.DataFrame, is_def: bool) -> pd.DataFrame:
    df = map_synonyms(df, is_def=is_def)

    if is_wide(df):
        df = melt_to_long(df)

    if "cie10_code" not in df.columns and "diagnostico" in df.columns:
        df = df.rename(columns={"diagnostico":"cie10_code"})

    if "valor" not in df.columns:
        df["valor"] = 1

    if "anio" not in df.columns:
        for cand in ["fecha","fecha_evento","fecha_defuncion","fecha_urgencia"]:
            if cand in df.columns:
                ser = pd.to_datetime(df[cand], errors="coerce")
                if ser.notna().any():
                    df["anio"] = ser.dt.year
                    break

    for col in ["anio","entidad_norm","sexo","cie10_code","valor"]:
        if col not in df.columns:
            df[col] = pd.NA

    df["anio"] = pd.to_numeric(df["anio"], errors="coerce")
    df["entidad_norm"] = df["entidad_norm"].astype(str).str.strip()
    df["sexo"] = df["sexo"].astype(str).str.strip()
    df["cie10_code"] = df["cie10_code"].astype(str).str.upper().str.strip()

    return df[["anio","entidad_norm","sexo","cie10_code","valor"]]

def perfilado(def_df: pd.DataFrame, urg_df: pd.DataFrame):
    paths = []

    cnt = pd.DataFrame({
        "tabla":["defunciones","urgencias"],
        "filas":[len(def_df), len(urg_df)]
    })
    p = os.path.join(OUTDIR, "01_conteos.csv"); cnt.to_csv(p, index=False); paths.append(p)

    def_nulos = def_df[["anio","entidad_norm","sexo","cie10_code"]].isna().sum().rename("defunciones")
    urg_nulos = urg_df[["anio","entidad_norm","sexo","cie10_code"]].isna().sum().rename("urgencias")
    nulos = pd.concat([def_nulos, urg_nulos], axis=1).reset_index(names="campo")
    p = os.path.join(OUTDIR, "02_nulos.csv"); nulos.to_csv(p, index=False); paths.append(p)

    rng = pd.DataFrame([
        {"tabla":"defunciones","min_anio":pd.to_numeric(def_df["anio"], errors="coerce").min(),
         "max_anio":pd.to_numeric(def_df["anio"], errors="coerce").max()},
        {"tabla":"urgencias","min_anio":pd.to_numeric(urg_df["anio"], errors="coerce").min(),
         "max_anio":pd.to_numeric(urg_df["anio"], errors="coerce").max()}
    ])
    p = os.path.join(OUTDIR, "03_rango_anios.csv"); rng.to_csv(p, index=False); paths.append(p)

    def_bad = ~(def_df["cie10_code"].astype(str).str.upper().str.strip().str.match(CIE_PATTERN))
    urg_bad = ~(urg_df["cie10_code"].astype(str).str.upper().str.strip().str.match(CIE_PATTERN))
    fuera = pd.DataFrame({
        "tabla":["defunciones","urgencias"],
        "fuera_de_rango":[int(def_bad.sum()), int(urg_bad.sum())]
    })
    p = os.path.join(OUTDIR, "04_fuera_rango.csv"); fuera.to_csv(p, index=False); paths.append(p)

    key = ["anio","entidad_norm","sexo","cie10_code"]
    def_dups = def_df[key].astype(str).duplicated(keep=False).sum()
    urg_dups = urg_df[key].astype(str).duplicated(keep=False).sum()
    dups = pd.DataFrame({
        "tabla":["defunciones","urgencias"],
        "filas_en_grupos_duplicados":[int(def_dups), int(urg_dups)]
    })
    p = os.path.join(OUTDIR, "05_duplicados.csv"); dups.to_csv(p, index=False); paths.append(p)

    def_sexo = def_df["sexo"].fillna("NULL").astype(str).str.strip().str.upper().value_counts(dropna=False).rename_axis("sexo").reset_index(name="defunciones")
    urg_sexo = urg_df["sexo"].fillna("NULL").astype(str).str.strip().str.upper().value_counts(dropna=False).rename_axis("sexo").reset_index(name="urgencias")
    sexo = pd.merge(def_sexo, urg_sexo, on="sexo", how="outer").fillna(0)
    p = os.path.join(OUTDIR, "06_distribucion_sexo.csv"); sexo.to_csv(p, index=False); paths.append(p)

    return paths

def main_csv():
    ensure_outdir()

    raw_def = read_csv_smart(IN_DEF)
    def_df = ensure_long_schema(raw_def, is_def=True)

    raw_urg = read_csv_smart(IN_URG)
    urg_df = ensure_long_schema(raw_urg, is_def=False)

    paths = perfilado(def_df, urg_df)

    md = os.path.join(OUTDIR, "perfilado_csv_sql_resumen.md")
    with open(md, "w", encoding="utf-8") as f:
        f.write("# Perfilado CSV – Hechos (defunciones/urgencias)\n\n")
        f.write(f"_Generado: {datetime.datetime.now().isoformat(timespec='seconds')}_\n\n")
        for p in paths:
            f.write(f"- {os.path.basename(p)}\n")
        f.write("\n")

    print(f"[OK] CSV perfilado (SQL) → {OUTDIR}")
    print("[INFO] Columnas procesadas (DEF):", list(def_df.columns))
    print("[INFO] Columnas procesadas (URG):", list(urg_df.columns))

if __name__ == "__main__":
    main_csv()
