#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Perfilado sobre CSV del grafo CIE-10.
Lee: Data/cie10_f10_f19_nodes.csv y Data/cie10_f10_f19_edges_enriched.csv
"""

import re, unicodedata, pandas as pd, os, datetime

IN_NODES = "Data/cie10_f10_f19_nodes.csv"
IN_EDGES = "Data/cie10_f10_f19_edges_enriched.csv"
OUTDIR_GRAFO = "docs/perfilado/grafo"

def ensure_outdir(path):
    os.makedirs(path, exist_ok=True)

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

def rename_first_match(df: pd.DataFrame, targets: list[str], new_name: str):
    for cand in targets:
        c = norm_header(cand)
        if c in df.columns:
            if c != new_name:
                df.rename(columns={c: new_name}, inplace=True)
            return True
    return False

def main_grafo():
    ensure_outdir(OUTDIR_GRAFO)

    nodes = read_csv_smart(IN_NODES)
    edges = read_csv_smart(IN_EDGES)

    if not rename_first_match(nodes,
        ["cie10_code","cie10","codigo_cie10","codigo","code","id","node","node_id"],
        "cie10_code"):
        nodes["cie10_code"] = pd.NA

    if not rename_first_match(nodes,
        ["descripcion","description","label","nombre","titulo","title","desc","name","concepto"],
        "descripcion"):
        nodes["descripcion"] = pd.NA

    if not rename_first_match(edges,
        ["source","origen","from","src","source_code","source_id","nodo_origen"],
        "source"):
        edges["source"] = pd.NA

    if not rename_first_match(edges,
        ["target","destino","to","dst","target_code","target_id","nodo_destino"],
        "target"):
        edges["target"] = pd.NA

    nodes["cie10_code"] = nodes["cie10_code"].astype(str).str.upper().str.strip()
    edges["source"]     = edges["source"].astype(str).str.upper().str.strip()
    edges["target"]     = edges["target"].astype(str).str.upper().str.strip()

    nodes["desc_empty"] = nodes["descripcion"].isna() | (nodes["descripcion"].astype(str).str.strip()=="")
    df_nodes_summary = pd.DataFrame({
        "total_nodos":[len(nodes)],
        "descripciones_nulas":[int(nodes["desc_empty"].sum())]
    })
    df_nodes_summary.to_csv(os.path.join(OUTDIR_GRAFO, "01_nodos.csv"), index=False, encoding="utf-8")

    edges_nulls = (edges["source"].isna() | (edges["source"]=="") | edges["target"].isna() | (edges["target"]=="")).sum()
    df_edges_summary = pd.DataFrame({
        "total_aristas":[len(edges)],
        "aristas_nulas":[int(edges_nulls)]
    })
    df_edges_summary.to_csv(os.path.join(OUTDIR_GRAFO, "02_aristas.csv"), index=False, encoding="utf-8")

    node_codes = set(nodes["cie10_code"].dropna().astype(str))
    orphan_mask = ~edges["source"].isin(node_codes) | ~edges["target"].isin(node_codes)
    df_orphans = pd.DataFrame({"aristas_huerfanas":[int(orphan_mask.sum())]})
    df_orphans.to_csv(os.path.join(OUTDIR_GRAFO, "03_aristas_huerfanas.csv"), index=False, encoding="utf-8")

    md = os.path.join(OUTDIR_GRAFO, "perfilado_csv_grafo_resumen.md")
    with open(md, "w", encoding="utf-8") as f:
        f.write("# Perfilado CSV – Grafo (CIE-10)\n\n")
        f.write(f"_Generado: {datetime.datetime.now().isoformat(timespec='seconds')}_\n\n")
        f.write("- 01_nodos.csv\n- 02_aristas.csv\n- 03_aristas_huerfanas.csv\n\n")
        f.write("## Diagnóstico rápido\n")
        f.write(f"- Columnas NODOS detectadas: {list(nodes.columns)}\n")
        f.write(f"- Columnas ARISTAS detectadas: {list(edges.columns)}\n")

    print("[OK] CSV perfilado (Grafo) →", OUTDIR_GRAFO)
    print("[INFO] NODOS cols:", list(nodes.columns))
    print("[INFO] ARISTAS cols:", list(edges.columns))

if __name__ == "__main__":
    main_grafo()
