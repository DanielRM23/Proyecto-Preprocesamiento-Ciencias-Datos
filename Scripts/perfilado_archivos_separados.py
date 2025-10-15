#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Perfilado sobre CSV de hechos (defunciones / urgencias) ANTES de cargar a la BD.
Lee: Data/defunciones_uso_sustancias_clean.csv, Data/urgencias_uso_sustancias_clean.csv
Genera métricas en docs/Entrega4_perfilado_csv/sql/
"""

#!/usr/bin/env python3
# -*- coding: utf-8 -*-


# import os, re, datetime, unicodedata
# import pandas as pd

# IN_DEF = "Data/defunciones_uso_sustancias.csv"
# IN_URG = "Data/urgencias_uso_sustancias.csv"
# OUTDIR = "docs/perfilado/csv"

# F_WIDE_PATTERN = re.compile(r"^f1[0-9]$")    # f10..f19 (normalizados)
# CIE_PATTERN    = re.compile(r"^f1[0-9](\..+)?$", re.IGNORECASE)  # p.ej. F14 o F14.2

# # ---------------- utilidades ----------------
# def ensure_outdir():
#     os.makedirs(OUTDIR, exist_ok=True)

# def strip_accents(s: str) -> str:
#     return "".join(c for c in unicodedata.normalize("NFKD", str(s)) if not unicodedata.combining(c))

# def norm_header(h: str) -> str:
#     s = strip_accents(h).lower().strip()
#     s = re.sub(r"[^\w]+", "_", s)
#     s = re.sub(r"_+", "_", s).strip("_")
#     return s

# def read_csv_smart(path: str) -> pd.DataFrame:
#     for enc in ("utf-8", "latin-1"):
#         try:
#             df = pd.read_csv(path, sep=None, engine="python", encoding=enc)
#             break
#         except Exception:
#             continue
#     else:
#         df = pd.read_csv(path, encoding="latin-1")
#     df.columns = [norm_header(c) for c in df.columns]
#     return df

# def map_synonyms(df: pd.DataFrame, is_def: bool) -> pd.DataFrame:
#     # candidatos
#     year_opts = ["anio","ano","año","year","anio_defuncion","ano_defuncion","anio_urgencia","ano_urgencia"]
#     ent_opts  = ["entidad_norm","entidad","entidad_defuncion_etq","entidad_defuncion","nom_ent","estado","nombre_entidad"]
#     sex_opts  = ["sexo","genero","género","sex"]
#     cie_opts  = ["cie10_code","cie10","codigo_cie10","dx_cie10","diagnostico","diagnostico_cie10","dx","codigo"]
#     val_opts  = ["valor","conteo","cantidad","total","n","eventos"]

#     # escoge primera col existente para cada campo
#     def pick(opts):
#         for o in opts:
#             o2 = norm_header(o)
#             if o2 in df.columns:
#                 return o2
#         return None

#     mapping = {
#         "anio": pick(year_opts),
#         "entidad_norm": pick(ent_opts),
#         "sexo": pick(sex_opts),
#         "cie10_code": pick(cie_opts),
#         "valor": pick(val_opts)
#     }
#     # renombrar lo que sí exista
#     rename = {v:k for k,v in mapping.items() if v and v != k}
#     if rename:
#         df = df.rename(columns=rename)

#     # para defunciones, si hay cve_entidad + etiqueta, preferir la etiqueta como entidad_norm
#     if is_def:
#         if "entidad_defuncion_etq" in df.columns and "entidad_norm" not in df.columns:
#             df = df.rename(columns={"entidad_defuncion_etq":"entidad_norm"})
#         elif "entidad_defuncion" in df.columns and "entidad_norm" not in df.columns:
#             df = df.rename(columns={"entidad_defuncion":"entidad_norm"})

#     return df

# def is_wide(df: pd.DataFrame) -> bool:
#     # formato ancho si existen columnas f10..f19
#     cols = set(df.columns)
#     return any(c for c in cols if F_WIDE_PATTERN.match(c))

# def melt_to_long(df: pd.DataFrame) -> pd.DataFrame:
#     """Convierte columnas f10..f19 a filas con cie10_code, valor"""
#     id_cols = [c for c in df.columns if not F_WIDE_PATTERN.match(c)]
#     fcols   = [c for c in df.columns if F_WIDE_PATTERN.match(c)]
#     long_df = df.melt(id_vars=id_cols, value_vars=fcols, var_name="cie10_code", value_name="valor")
#     # normalizar: f10 -> F10
#     long_df["cie10_code"] = long_df["cie10_code"].str.upper()
#     # filtrar vacíos/0 si aplica
#     if "valor" in long_df.columns:
#         long_df["valor"] = pd.to_numeric(long_df["valor"], errors="coerce")
#         long_df = long_df[long_df["valor"].notna()]
#     return long_df

# def ensure_long_schema(df: pd.DataFrame, is_def: bool) -> pd.DataFrame:
#     """Asegura columnas: anio, entidad_norm, sexo, cie10_code, valor (crea/convierte si hace falta)."""
#     # mapear sinónimos conocidos
#     df = map_synonyms(df, is_def=is_def)

#     # Si está ancho, derramar
#     if is_wide(df):
#         df = melt_to_long(df)

#     # Si no hay 'cie10_code' pero hay 'diagnostico' con valores tipo F14, renombrar
#     if "cie10_code" not in df.columns and "diagnostico" in df.columns:
#         df = df.rename(columns={"diagnostico":"cie10_code"})

#     # Crear 'valor' si no existe (asignar 1 por fila como conteo)
#     if "valor" not in df.columns:
#         df["valor"] = 1

#     # Intentar derivar 'anio' si viene como fecha
#     if "anio" not in df.columns:
#         # buscar columnas tipo fecha
#         for cand in ["fecha","fecha_evento","fecha_defuncion","fecha_urgencia"]:
#             if cand in df.columns:
#                 ser = pd.to_datetime(df[cand], errors="coerce")
#                 if ser.notna().any():
#                     df["anio"] = ser.dt.year
#                     break

#     # llenar faltantes razonables
#     for col in ["anio","entidad_norm","sexo","cie10_code","valor"]:
#         if col not in df.columns:
#             df[col] = pd.NA

#     # normalizaciones ligeras
#     df["anio"] = pd.to_numeric(df["anio"], errors="coerce")
#     df["entidad_norm"] = df["entidad_norm"].astype(str).str.strip()
#     df["sexo"] = df["sexo"].astype(str).str.strip()
#     df["cie10_code"] = df["cie10_code"].astype(str).str.upper().str.strip()

#     return df[["anio","entidad_norm","sexo","cie10_code","valor"]]

# # ---------------- perfilado ----------------
# def perfilado(def_df: pd.DataFrame, urg_df: pd.DataFrame):
#     paths = []

#     # 1) Conteos
#     cnt = pd.DataFrame({
#         "tabla":["defunciones","urgencias"],
#         "filas":[len(def_df), len(urg_df)]
#     })
#     p = os.path.join(OUTDIR, "01_conteos.csv"); cnt.to_csv(p, index=False); paths.append(p)

#     # 2) Nulos
#     def_nulos = def_df[["anio","entidad_norm","sexo","cie10_code"]].isna().sum().rename("defunciones")
#     urg_nulos = urg_df[["anio","entidad_norm","sexo","cie10_code"]].isna().sum().rename("urgencias")
#     nulos = pd.concat([def_nulos, urg_nulos], axis=1).reset_index(names="campo")
#     p = os.path.join(OUTDIR, "02_nulos.csv"); nulos.to_csv(p, index=False); paths.append(p)

#     # 3) Rango de años
#     rng = pd.DataFrame([
#         {"tabla":"defunciones","min_anio":pd.to_numeric(def_df["anio"], errors="coerce").min(),
#          "max_anio":pd.to_numeric(def_df["anio"], errors="coerce").max()},
#         {"tabla":"urgencias","min_anio":pd.to_numeric(urg_df["anio"], errors="coerce").min(),
#          "max_anio":pd.to_numeric(urg_df["anio"], errors="coerce").max()}
#     ])
#     p = os.path.join(OUTDIR, "03_rango_anios.csv"); rng.to_csv(p, index=False); paths.append(p)

#     # 4) Códigos fuera de F10–F19
#     def_bad = ~(def_df["cie10_code"].astype(str).str.upper().str.strip().str.match(CIE_PATTERN))
#     urg_bad = ~(urg_df["cie10_code"].astype(str).str.upper().str.strip().str.match(CIE_PATTERN))
#     fuera = pd.DataFrame({
#         "tabla":["defunciones","urgencias"],
#         "fuera_de_rango":[int(def_bad.sum()), int(urg_bad.sum())]
#     })
#     p = os.path.join(OUTDIR, "04_fuera_rango.csv"); fuera.to_csv(p, index=False); paths.append(p)

#     # 5) Duplicados por clave (ancho ya convertido a largo)
#     key = ["anio","entidad_norm","sexo","cie10_code"]
#     def_dups = def_df[key].astype(str).duplicated(keep=False).sum()
#     urg_dups = urg_df[key].astype(str).duplicated(keep=False).sum()
#     dups = pd.DataFrame({
#         "tabla":["defunciones","urgencias"],
#         "filas_en_grupos_duplicados":[int(def_dups), int(urg_dups)]
#     })
#     p = os.path.join(OUTDIR, "05_duplicados.csv"); dups.to_csv(p, index=False); paths.append(p)

#     # 6) Distribución de sexo
#     def_sexo = def_df["sexo"].fillna("NULL").astype(str).str.strip().str.upper().value_counts(dropna=False).rename_axis("sexo").reset_index(name="defunciones")
#     urg_sexo = urg_df["sexo"].fillna("NULL").astype(str).str.strip().str.upper().value_counts(dropna=False).rename_axis("sexo").reset_index(name="urgencias")
#     sexo = pd.merge(def_sexo, urg_sexo, on="sexo", how="outer").fillna(0)
#     p = os.path.join(OUTDIR, "06_distribucion_sexo.csv"); sexo.to_csv(p, index=False); paths.append(p)

#     return paths

# def main_csv():
#     ensure_outdir()

#     # DEFUNCIONES
#     raw_def = read_csv_smart(IN_DEF)
#     def_df = ensure_long_schema(raw_def, is_def=True)

#     # URGENCIAS
#     raw_urg = read_csv_smart(IN_URG)
#     urg_df = ensure_long_schema(raw_urg, is_def=False)

#     # Perfilado
#     paths = perfilado(def_df, urg_df)

#     # Resumen MD
#     md = os.path.join(OUTDIR, "perfilado_csv_sql_resumen.md")
#     with open(md, "w", encoding="utf-8") as f:
#         f.write("# Perfilado CSV – Hechos (defunciones/urgencias)\n\n")
#         f.write(f"_Generado: {datetime.datetime.now().isoformat(timespec='seconds')}_\n\n")
#         for p in paths:
#             f.write(f"- {os.path.basename(p)}\n")
#         f.write("\n")

#     print(f"[OK] CSV perfilado (SQL) → {OUTDIR}")
#     print("[INFO] Columnas procesadas (DEF):", list(def_df.columns))
#     print("[INFO] Columnas procesadas (URG):", list(urg_df.columns))



#====================================================================================================================================

# import re, unicodedata, pandas as pd, os, datetime

# IN_NODES = "Data/cie10_f10_f19_nodes.csv"
# IN_EDGES = "Data/cie10_f10_f19_edges_enriched.csv"
# OUTDIR_GRAFO = "docs/perfilado/grafo"

# def ensure_outdir(path):
#     os.makedirs(path, exist_ok=True)

# def strip_accents(s: str) -> str:
#     return "".join(c for c in unicodedata.normalize("NFKD", str(s)) if not unicodedata.combining(c))

# def norm_header(h: str) -> str:
#     s = strip_accents(h).lower().strip()
#     s = re.sub(r"[^\w]+", "_", s)
#     s = re.sub(r"_+", "_", s).strip("_")
#     return s

# def read_csv_smart(path: str) -> pd.DataFrame:
#     for enc in ("utf-8", "latin-1"):
#         try:
#             df = pd.read_csv(path, sep=None, engine="python", encoding=enc)
#             break
#         except Exception:
#             continue
#     else:
#         df = pd.read_csv(path, encoding="latin-1")
#     df.columns = [norm_header(c) for c in df.columns]
#     return df

# def rename_first_match(df: pd.DataFrame, targets: list[str], new_name: str):
#     """Si alguna columna de 'targets' existe, la renombra a 'new_name' y devuelve True."""
#     for cand in targets:
#         c = norm_header(cand)
#         if c in df.columns:
#             if c != new_name:
#                 df.rename(columns={c: new_name}, inplace=True)
#             return True
#     return False

# # ==== GRAFO: robustecer mapeo de columnas y perfilar ====
# def main_grafo():
#     ensure_outdir(OUTDIR_GRAFO)

#     nodes = read_csv_smart(IN_NODES)
#     edges = read_csv_smart(IN_EDGES)

#     # ---- Mapear columnas de NODOS ----
#     # clave del nodo (cie10_code)
#     if not rename_first_match(nodes,
#         ["cie10_code","cie10","codigo_cie10","codigo","code","id","node","node_id"],
#         "cie10_code"):
#         # si no existe, creamos vacía para evitar KeyError (y que el perfilado te lo marque)
#         nodes["cie10_code"] = pd.NA

#     # descripción del nodo
#     if not rename_first_match(nodes,
#         ["descripcion","description","label","nombre","titulo","title","desc","name","concepto"],
#         "descripcion"):
#         nodes["descripcion"] = pd.NA

#     # ---- Mapear columnas de ARISTAS ----
#     if not rename_first_match(edges,
#         ["source","origen","from","src","source_code","source_id","nodo_origen"],
#         "source"):
#         edges["source"] = pd.NA

#     if not rename_first_match(edges,
#         ["target","destino","to","dst","target_code","target_id","nodo_destino"],
#         "target"):
#         edges["target"] = pd.NA

#     # Normalizaciones ligeras
#     nodes["cie10_code"] = nodes["cie10_code"].astype(str).str.upper().str.strip()
#     edges["source"]     = edges["source"].astype(str).str.upper().str.strip()
#     edges["target"]     = edges["target"].astype(str).str.upper().str.strip()

#     # ---- Métricas del perfilado (robustas) ----
#     # 01: Nodos y descripciones nulas
#     nodes["desc_empty"] = nodes["descripcion"].isna() | (nodes["descripcion"].astype(str).str.strip()=="")
#     df_nodes_summary = pd.DataFrame({
#         "total_nodos":[len(nodes)],
#         "descripciones_nulas":[int(nodes["desc_empty"].sum())]
#     })
#     df_nodes_summary.to_csv(os.path.join(OUTDIR_GRAFO, "01_nodos.csv"), index=False, encoding="utf-8")

#     # 02: Aristas con nulos en source/target
#     edges_nulls = (edges["source"].isna() | (edges["source"]=="") | edges["target"].isna() | (edges["target"]=="")).sum()
#     df_edges_summary = pd.DataFrame({
#         "total_aristas":[len(edges)],
#         "aristas_nulas":[int(edges_nulls)]
#     })
#     df_edges_summary.to_csv(os.path.join(OUTDIR_GRAFO, "02_aristas.csv"), index=False, encoding="utf-8")

#     # 03: Aristas huérfanas (source/target no existen como nodo)
#     node_codes = set(nodes["cie10_code"].dropna().astype(str))
#     orphan_mask = ~edges["source"].isin(node_codes) | ~edges["target"].isin(node_codes)
#     df_orphans = pd.DataFrame({"aristas_huerfanas":[int(orphan_mask.sum())]})
#     df_orphans.to_csv(os.path.join(OUTDIR_GRAFO, "03_aristas_huerfanas.csv"), index=False, encoding="utf-8")

#     # Resumen Markdown
#     md = os.path.join(OUTDIR_GRAFO, "perfilado_csv_grafo_resumen.md")
#     with open(md, "w", encoding="utf-8") as f:
#         f.write("# Perfilado CSV – Grafo (CIE-10)\n\n")
#         f.write(f"_Generado: {datetime.datetime.now().isoformat(timespec='seconds')}_\n\n")
#         f.write("- 01_nodos.csv\n- 02_aristas.csv\n- 03_aristas_huerfanas.csv\n\n")
#         f.write("## Diagnóstico rápido\n")
#         f.write(f"- Columnas NODOS detectadas: {list(nodes.columns)}\n")
#         f.write(f"- Columnas ARISTAS detectadas: {list(edges.columns)}\n")

#     print("[OK] CSV perfilado (Grafo) →", OUTDIR_GRAFO)
#     print("[INFO] NODOS cols:", list(nodes.columns))
#     print("[INFO] ARISTAS cols:", list(edges.columns))


# ====================================================================================================================================

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Perfilado sobre CSV de texto (CoWeSe) ANTES de cargar a la BD.
Lee: Textos/cowese_matches.csv
"""

# ==== Utilidades mínimas (usa las tuyas si ya las tienes) ====
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

# ==== TEXTO: perfilado robusto ====
IN_TEXT = "Textos/cowese_matches.csv"
OUTDIR_TEXTO = "docs/Perfilado/texto"
CIE_PATTERN = re.compile(r"^F1[0-9](\..+)?$", re.IGNORECASE)

def main_texto():
    os.makedirs(OUTDIR_TEXTO, exist_ok=True)

    t = read_csv_smart(IN_TEXT)

    # Mapear columnas clave
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

    # Si faltan, crearlas para no romper (y dejar evidencia en el resumen)
    if not has_sentence:
        t["sentence"] = pd.NA
    if not has_cie:
        t["cie10_code"] = pd.NA

    # Normalizaciones
    t["sentence"] = t["sentence"].astype(str)
    t["cie10_code"] = t["cie10_code"].astype(str).str.upper().str.strip()

    # 01) Conteo y frases vacías
    sent_empty = (t["sentence"].isna()) | (t["sentence"].astype(str).str.strip() == "")
    df_conteos = pd.DataFrame({
        "total_filas":[len(t)],
        "frases_vacias":[int(sent_empty.sum())]
    })
    df_conteos.to_csv(os.path.join(OUTDIR_TEXTO, "01_conteos_frases.csv"), index=False, encoding="utf-8")

    # 02) Duplicados por (sentence, cie10_code)
    dup = t.duplicated(subset=["sentence","cie10_code"], keep=False)
    df_dups = pd.DataFrame({"filas_en_grupos_duplicados":[int(dup.sum())]})
    df_dups.to_csv(os.path.join(OUTDIR_TEXTO, "02_duplicados.csv"), index=False, encoding="utf-8")

    # 03) Códigos fuera de rango F10–F19
    fuera = ~(t["cie10_code"].fillna("").str.match(CIE_PATTERN))
    df_fuera = pd.DataFrame({"fuera_de_rango":[int(fuera.sum())]})
    df_fuera.to_csv(os.path.join(OUTDIR_TEXTO, "03_fuera_rango.csv"), index=False, encoding="utf-8")

    # Resumen Markdown
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
    # main_csv()
    # main_grafo()
    main_texto()
