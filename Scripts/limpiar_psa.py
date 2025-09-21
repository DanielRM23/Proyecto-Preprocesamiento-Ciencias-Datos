# limpiar_psa.py
# Limpia y normaliza los CSV de defunciones y urgencias por uso de sustancias (F10..F19).
# - Rellena NaN en F10..F19 con 0 y castea a int
# - Parsea 'fecha' a datetime
# - Valida columnas esperadas
# - Guarda *_clean.csv en la misma carpeta

import pandas as pd
from pathlib import Path

DATA_DIR = Path("./Data")    # ajustar si tu ruta es distinta
DEF_FILE = DATA_DIR / "defunciones_uso_sustancias.csv"
URG_FILE = DATA_DIR / "urgencias_uso_sustancias.csv"

DEF_OUT  = DATA_DIR / "defunciones_uso_sustancias_clean.csv"
URG_OUT  = DATA_DIR / "urgencias_uso_sustancias_clean.csv"

FX_COLS = ["F10","F11","F12","F13","F14","F15","F16","F17","F18","F19"]

def check_columns(df, required, name):
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"[{name}] Faltan columnas: {missing}")

def limpiar_defunciones(path_in: Path, path_out: Path):
    print(f"Cargando: {path_in}")
    df = pd.read_csv(path_in)

    # Columnas esperadas mínimas (ajusta si necesitas)
    required = ["anio_defuncion","entidad_defuncion","edad_quinquenal","sexo","fecha",
                "cve_entidad","entidad_defuncion_etq"] + FX_COLS
    check_columns(df, required, "defunciones")

    # Limpiar F10..F19
    df[FX_COLS] = df[FX_COLS].fillna(0)
    # Si hay valores no numéricos raros, fuerza a numérico seguro
    for c in FX_COLS:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype("int64")

    # Parsear fecha
    df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce").dt.date

    # Orden sugerido de columnas (opcional)
    ordered = ["anio_defuncion","entidad_defuncion","edad_quinquenal","sexo"] + FX_COLS + \
              ["cve_entidad","fecha","entidad_defuncion_etq"]
    df = df[[c for c in ordered if c in df.columns]]

    # Resumen rápido
    print("\n[defunciones] Resumen FXX:")
    print(df[FX_COLS].sum().sort_values(ascending=False))

    df.to_csv(path_out, index=False)
    print(f"Guardado limpio: {path_out}")

def limpiar_urgencias(path_in: Path, path_out: Path):
    print(f"\nCargando: {path_in}")
    df = pd.read_csv(path_in)

    # Columnas esperadas mínimas (ajusta si necesitas)
    required = ["anio","entidad","edad_quinquenal","sexo","fecha"] + FX_COLS
    check_columns(df, required, "urgencias")

    # Limpiar F10..F19
    df[FX_COLS] = df[FX_COLS].fillna(0)
    for c in FX_COLS:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype("int64")

    # Parsear fecha
    df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce").dt.date

    # Orden sugerido de columnas (opcional)
    ordered = ["anio","entidad","edad_quinquenal","sexo"] + FX_COLS + ["fecha"]
    df = df[[c for c in ordered if c in df.columns]]

    # Resumen rápido
    print("\n[urgencias] Resumen FXX:")
    print(df[FX_COLS].sum().sort_values(ascending=False))

    df.to_csv(path_out, index=False)
    print(f"Guardado limpio: {path_out}")

if __name__ == "__main__":
    limpiar_defunciones(DEF_FILE, DEF_OUT)
    limpiar_urgencias(URG_FILE, URG_OUT)

    print("\n✓ Listo. Puedes cargar estos archivos a Postgres con \\COPY o usarlos directo en pandas.")
