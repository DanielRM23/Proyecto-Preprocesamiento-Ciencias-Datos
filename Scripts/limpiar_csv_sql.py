#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Limpieza de duplicados para CSV de hechos (defunciones / urgencias) CONSERVANDO EDAD.

- Soporta formato ANCHO (f10..f19) y LARGO (cie10_code, valor)
- Normaliza encabezados (minúsculas, sin acentos, _)
- Mapea sinónimos -> anio, entidad_norm, sexo, edad_quinquenal, cie10_code, valor
- Convierte a largo si es necesario y AGREGA por (anio, entidad_norm, sexo, edad_quinquenal, cie10_code)
- Guarda *_dedup.csv y genera log con métricas antes/después

Entradas (por defecto):
  Data/defunciones_uso_sustancias_clean.csv
  Data/urgencias_uso_sustancias_clean.csv

Salidas:
  Data/defunciones_uso_sustancias_dedup.csv
  Data/urgencias_uso_sustancias_dedup.csv
  docs/Entrega4_limpieza_csv_sql_log.md
"""

import os, re, datetime, unicodedata
import pandas as pd

# ================== Config por defecto (puedes cambiarlas) ===================
IN_DEF = "Data/defunciones_uso_sustancias_clean.csv"
IN_URG = "Data/urgencias_uso_sustancias_clean.csv"
OUT_DEF = "Data/Limpieza/defunciones_uso_sustancias_dedup.csv"
OUT_URG = "Data/Limpieza/urgencias_uso_sustancias_dedup.csv"
LOG_MD = "docs/Limpieza/limpieza_csv_sql_log.md"

# F10..F19 (normalizados) en formato ancho
F_WIDE_PATTERN = re.compile(r"^f1[0-9]$")                 # f10..f19
# Códigos CIE-10 válidos del dominio (ej. F14 o F14.2)
CIE_PATTERN    = re.compile(r"^F1[0-9](\..+)?$", re.I)

# ======== Opcionales de seguridad semántica (actívalos si los quieres) =======
FILTER_TO_F10_F19 = True   # Filtrar estrictamente a F10–F19 tras normalizar
DROP_ZERO_VALS    = False  # Eliminar filas con valor <= 0 (usa True si 0 == “sin dato”)

# ============================ Utilidades básicas ==============================
def ensure_dir(path: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)

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

def pick_first(df: pd.DataFrame, candidates: list[str]) -> str | None:
    for c in candidates:
        c2 = norm_header(c)
        if c2 in df.columns:
            return c2
    return None

def is_wide(df: pd.DataFrame) -> bool:
    return any(F_WIDE_PATTERN.match(c) for c in df.columns)

def melt_to_long(df: pd.DataFrame) -> pd.DataFrame:
    id_cols = [c for c in df.columns if not F_WIDE_PATTERN.match(c)]
    fcols   = [c for c in df.columns if F_WIDE_PATTERN.match(c)]
    long_df = df.melt(id_vars=id_cols, value_vars=fcols, var_name="cie10_code", value_name="valor")
    long_df["cie10_code"] = long_df["cie10_code"].astype(str).str.upper().str.strip()
    # convertir valor a numérico (coerce NaN → 0)
    long_df["valor"] = pd.to_numeric(long_df["valor"], errors="coerce").fillna(0)
    return long_df

# ====================== Estandarización a formato largo ======================
def to_long_and_standardize(df: pd.DataFrame, is_def: bool) -> pd.DataFrame:
    """
    Devuelve DF con columnas:
    anio, entidad_norm, sexo, edad_quinquenal, cie10_code, valor
    """
    # Mapeo flexible de sinónimos
    year = pick_first(df, [
        "anio","ano","año","year","anio_defuncion","ano_defuncion",
        "anio_urgencia","ano_urgencia","periodo","fecha"
    ])
    ent  = pick_first(df, [
        "entidad_norm","entidad_defuncion_etq","entidad_defuncion","entidad",
        "nom_ent","estado","nombre_entidad","entidad_urgencia"
    ])
    sex  = pick_first(df, ["sexo","genero","género","sex"])
    age  = pick_first(df, ["edad_quinquenal","grupo_edad","edad","edad_grupo","rango_edad"])
    cie  = pick_first(df, ["cie10_code","cie10","codigo_cie10","dx_cie10","diagnostico_cie10","diagnostico","codigo","dx"])
    val  = pick_first(df, ["valor","conteo","cantidad","total","n","eventos","frecuencia"])

    # Si está ANCHO, derramar a LARGO
    if is_wide(df):
        df = melt_to_long(df)
        # tras melt, ya tenemos 'cie10_code' y 'valor'
        cie, val = "cie10_code", "valor"
        # heurísticas para detectar año/entidad/sexo/edad tras melt
        if year is None:
            year = pick_first(df, ["anio","ano","año","year","anio_defuncion","anio_urgencia"])
        if ent is None:
            ent = pick_first(df, ["entidad_norm","entidad_defuncion_etq","entidad_defuncion","entidad","nom_ent","estado"])
        if sex is None:
            sex = pick_first(df, ["sexo","genero","género","sex"])
        if age is None:
            age = pick_first(df, ["edad_quinquenal","grupo_edad","edad","edad_grupo","rango_edad"])

    # Si aún falta 'valor', usar 1 por fila (conteo por ocurrencia)
    if val is None:
        df["valor"] = 1
        val = "valor"

    # Si el año viene como fecha, extraer año
    if year is None:
        date_col = pick_first(df, ["fecha","fecha_evento","fecha_defuncion","fecha_urgencia"])
        if date_col:
            ser = pd.to_datetime(df[date_col], errors="coerce")
            df["anio"] = ser.dt.year
            year = "anio"

    # Si entidad_norm no existe pero hay etiqueta de entidad, úsala
    if ent is None and is_def and "entidad_defuncion_etq" in df.columns:
        ent = "entidad_defuncion_etq"

    # Crear/renombrar columnas estándar
    spec = [
        ("anio", year),
        ("entidad_norm", ent),
        ("sexo", sex),
        ("edad_quinquenal", age),
        ("cie10_code", cie),
        ("valor", val),
    ]
    for col, src in spec:
        if src is None:
            df[col] = pd.NA
        elif src != col:
            df[col] = df[src]

    # Normalizaciones de tipo y texto
    df["anio"] = pd.to_numeric(df["anio"], errors="coerce")
    df["entidad_norm"] = df["entidad_norm"].astype(str).str.strip()
    df["sexo"] = df["sexo"].astype(str).str.strip()
    df["edad_quinquenal"] = df["edad_quinquenal"].astype(str).str.strip()
    df["cie10_code"] = df["cie10_code"].astype(str).str.upper().str.strip()
    df["valor"] = pd.to_numeric(df["valor"], errors="coerce").fillna(0)

    # Filtros opcionales
    if FILTER_TO_F10_F19:
        df = df[df["cie10_code"].str.match(CIE_PATTERN, na=False)]
    if DROP_ZERO_VALS:
        df = df[df["valor"] > 0]

    return df[["anio","entidad_norm","sexo","edad_quinquenal","cie10_code","valor"]]

# ============================= Métricas de dups ==============================
def summarize_dups(df: pd.DataFrame) -> int:
    key = ["anio","entidad_norm","sexo","edad_quinquenal","cie10_code"]
    return int(df[key].astype(str).duplicated(keep=False).sum())

# ============================ Agregación por clave ===========================
def aggregate_by_key(df: pd.DataFrame) -> pd.DataFrame:
    key = ["anio","entidad_norm","sexo","edad_quinquenal","cie10_code"]
    g = df.groupby(key, as_index=False)["valor"].sum()
    g = g[g["valor"].notna()]
    return g

# ================================= Pipeline =================================
def process_one(in_path: str, out_path: str, is_def: bool):
    ensure_dir(out_path)
    raw = read_csv_smart(in_path)
    long = to_long_and_standardize(raw, is_def=is_def)

    before_rows = len(long)
    before_dups = summarize_dups(long)

    cleaned = aggregate_by_key(long)

    after_rows = len(cleaned)
    after_dups = summarize_dups(cleaned)

    cleaned.to_csv(out_path, index=False, encoding="utf-8")

    return {
        "in": in_path,
        "out": out_path,
        "before_rows": before_rows,
        "before_dups": before_dups,
        "after_rows": after_rows,
        "after_dups": after_dups,
        "cols": list(long.columns)
    }

def main():
    ensure_dir(LOG_MD)

    res_def = process_one(IN_DEF, OUT_DEF, is_def=True)
    res_urg = process_one(IN_URG, OUT_URG, is_def=False)

    with open(LOG_MD, "w", encoding="utf-8") as f:
        f.write("# Limpieza CSV – Hechos (defunciones/urgencias) conservando edad\n\n")
        f.write(f"_Generado: {datetime.datetime.now().isoformat(timespec='seconds')}_\n\n")

        def section(title, res):
            f.write(f"## {title}\n")
            f.write(f"- Entrada: `{res['in']}`\n")
            f.write(f"- Salida:  `{res['out']}`\n")
            f.write(f"- Filas (antes → después): {res['before_rows']} → {res['after_rows']}\n")
            f.write(f"- Filas en grupos duplicados (antes → después): {res['before_dups']} → {res['after_dups']}\n")
            f.write(f"- Columnas estandarizadas (previas a agregar): {res['cols']}\n\n")

        section("Defunciones", res_def)
        section("Urgencias",   res_urg)

    print("[OK] Limpieza de CSV terminada (conservando edad).")
    print(f"  - Defunciones → {OUT_DEF} (filas {res_def['before_rows']}→{res_def['after_rows']}, dups {res_def['before_dups']}→{res_def['after_dups']})")
    print(f"  - Urgencias   → {OUT_URG} (filas {res_urg['before_rows']}→{res_urg['after_rows']}, dups {res_urg['before_dups']}→{res_urg['after_dups']})")
    print(f"  - Log         → {LOG_MD}")

if __name__ == "__main__":
    main()
