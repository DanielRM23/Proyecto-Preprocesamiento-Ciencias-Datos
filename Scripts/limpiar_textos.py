#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, re, unicodedata, datetime, hashlib, sys
import pandas as pd
from pathlib import Path

# ================== Config ==================
# Insumo principal (se intenta también la ruta alternativa si no existe)
IN_PATHS = [
    "Data/Textos/cowese_matches.csv",   # preferido (pipeline actual)
    "Textos/cowese_matches.csv",        # compatibilidad hacia atrás
]

OUT_PHRASES  = "Data/Textos/textos_cie10_frases.csv"         # frases únicas
OUT_MAP      = "Data/Textos/textos_cie10_frases_x_docs.csv"  # mapeo frase↔doc/sent
LOG_MD       = "docs/Entrega4_limpieza_textos_log.md"        # log Markdown

# Columnas candidatas
SENTENCE_CANDS = ["sentence","sentencia","texto","frase","sentence_text","descripcion"]
CODE_CANDS     = ["cie10_code","cie10","codigo_cie10","dx_cie10","diagnostico","codigo","dx"]
DOC_CANDS      = ["doc_id","document_id","id_doc","doc"]
SENT_CANDS     = ["sent_id","sentence_id","id_sent","id_oracion","sent"]

CIE_PATTERN = re.compile(r"^F1[0-9](\..+)?$", re.I)

# ================== Utils ==================
def ensure_parent(path: str | Path):
    """Crea el directorio padre del path si no existe."""
    p = Path(path)
    if p.parent.as_posix() not in (".", ""):
        p.parent.mkdir(parents=True, exist_ok=True)

def read_csv_smart(paths):
    """Intenta leer el primer CSV existente, auto-detectando separador y encoding."""
    if isinstance(paths, (str, Path)):
        paths = [paths]
    existing = next((Path(p) for p in paths if Path(p).exists()), None)
    if not existing:
        raise FileNotFoundError(f"No se encontró ningún insumo en: {paths}")
    for enc in ("utf-8", "latin-1"):
        try:
            return pd.read_csv(existing, sep=None, engine="python", encoding=enc)
        except Exception:
            continue
    return pd.read_csv(existing, encoding="latin-1")

def pick_first(df, cands):
    """Devuelve el primer nombre de columna presente (case-insensitive)."""
    cols = {c.lower(): c for c in df.columns}
    for c in cands:
        if c.lower() in cols:
            return cols[c.lower()]
    return None

def strip_accents(s):
    return "".join(c for c in unicodedata.normalize("NFKD", str(s)) if not unicodedata.combining(c))

def normalize_sentence(s: str) -> str:
    if pd.isna(s):
        return ""
    s = str(s)
    s = strip_accents(s).lower()
    s = re.sub(r"[^\w\s]", " ", s)   # quita puntuación suave
    s = re.sub(r"\s+", " ", s).strip()
    return s

def sha1_hex(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()

# ================== Main ==================
def main():
    # Asegura carpetas de salida
    for path in (OUT_PHRASES, OUT_MAP, LOG_MD):
        ensure_parent(path)

    # Leer insumo
    try:
        df = read_csv_smart(IN_PATHS)
    except Exception as e:
        print(f"[ERROR] No se pudo leer el insumo de texto: {e}")
        sys.exit(1)

    # Detectar columnas base
    sent_col = pick_first(df, SENTENCE_CANDS)
    code_col = pick_first(df, CODE_CANDS)
    doc_col  = pick_first(df, DOC_CANDS)
    sid_col  = pick_first(df, SENT_CANDS)

    if not sent_col or not code_col:
        print("[ERROR] No se detectaron columnas de texto o código.")
        print(" Columnas disponibles:", list(df.columns))
        sys.exit(1)

    # Columnas mínimas doc/sent
    if not doc_col:
        df["doc_id"] = 0
        doc_col = "doc_id"
    if not sid_col:
        # sent_id por grupo de doc
        df["sent_id"] = df.groupby(doc_col).cumcount()
        sid_col = "sent_id"

    # ===== Perfilado inicial =====
    total_filas = len(df)
    frases_vacias = int((df[sent_col].astype(str).str.strip() == "").sum())
    fuera_de_rango = int(
        (~df[code_col].astype(str).str.upper().str.strip().str.match(CIE_PATTERN, na=False)).sum()
    )

    # ===== Normalización =====
    df["_sentence_norm"] = df[sent_col].apply(normalize_sentence)
    df["_code_norm"] = df[code_col].astype(str).str.upper().str.strip()

    # Filtrar dominio CIE-10 y frases vacías (post-normalización)
    df = df[df["_code_norm"].str.match(CIE_PATTERN, na=False)]
    df = df[df["_sentence_norm"] != ""]

    # ===== 1) Quitar duplicados técnicos (doc_id, sent_id) =====
    before_dups_doc_sent = int(df[[doc_col, sid_col]].duplicated(keep=False).sum())
    df = df.drop_duplicates(subset=[doc_col, sid_col], keep="first")

    # ===== 2) Deduplicar a nivel frase (manteniendo procedencia) =====
    # clave: (codigo_normalizado || frase_normalizada)
    df["phrase_key"]  = df.apply(lambda r: f"{r['_code_norm']}||{r['_sentence_norm']}", axis=1)
    df["phrase_hash"] = df["phrase_key"].apply(sha1_hex)   # <<< NOMBRE FINAL CORRECTO

    # Mapeo frase↔doc/sent (N–a–N sin duplicados)
    map_df = df[["phrase_hash", doc_col, sid_col]].drop_duplicates()
    map_df = map_df.rename(columns={doc_col: "doc_id", sid_col: "sent_id"})

    # Frases únicas + conteo de ocurrencias
    phrases_df = (
        df.sort_values(["_code_norm", "_sentence_norm"])
          .drop_duplicates(subset=["phrase_hash"])
          .rename(columns={sent_col: "sentence_raw"})
    )
    counts = df.groupby("phrase_hash").size().rename("n_ocurrencias").reset_index()
    phrases_df = phrases_df.merge(counts, on="phrase_hash", how="left")

    # Ensamble columnas de salida (orden estable)
    phrases_out = phrases_df[["phrase_hash", "_code_norm", "sentence_raw", "_sentence_norm", "n_ocurrencias"]]
    phrases_out = phrases_out.rename(columns={"_code_norm": "cie10_code", "_sentence_norm": "sentence_norm"})

    # ===== Guardar =====
    phrases_out.to_csv(OUT_PHRASES, index=False, encoding="utf-8")
    map_df.to_csv(OUT_MAP, index=False, encoding="utf-8")

    # ===== Log =====
    with open(LOG_MD, "w", encoding="utf-8") as f:
        f.write("# Limpieza de Textos – con procedencia (frase ↔ doc/sent)\n\n")
        f.write(f"_Generado: {datetime.datetime.now().isoformat(timespec='seconds')}_\n\n")
        f.write("## Perfilado inicial\n")
        f.write(f"- total_filas: {total_filas}\n")
        f.write(f"- frases_vacias (previas): {frases_vacias}\n")
        f.write(f"- fuera_de_rango (previas): {fuera_de_rango}\n\n")
        f.write("## Duplicados\n")
        f.write(f"- Duplicados técnicos (doc_id, sent_id): {before_dups_doc_sent}\n")
        f.write(f"- Frases únicas finales: {len(phrases_out)}\n")
        f.write(f"- Mapeos frase↔doc/sent: {len(map_df)}\n\n")
        f.write("## Archivos de salida\n")
        f.write(f"- Frases únicas: `{OUT_PHRASES}`\n")
        f.write(f"- Mapeo N–a–N: `{OUT_MAP}`\n")

    print("[OK] Limpieza de textos completada ✅")
    print(f" - Frases únicas: {OUT_PHRASES}  (filas={len(phrases_out)})")
    print(f" - Mapeo frase↔doc/sent: {OUT_MAP}  (filas={len(map_df)})")
    print(f" - Log: {LOG_MD}")

if __name__ == "__main__":
    main()
