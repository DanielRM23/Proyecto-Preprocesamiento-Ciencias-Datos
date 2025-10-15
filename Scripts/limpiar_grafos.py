#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Limpieza robusta de grafo CIE-10 (nodos y aristas) con detección de sinónimos.

- Normaliza encabezados y texto
- Mapea columnas con nombres variables (code/id, source/from, target/to, rel_type/type/label, weight/peso)
- Elimina duplicados con las columnas disponibles
- Verifica consistencia: source/target deben existir en nodes
- Genera log con métricas
"""

import pandas as pd
import unicodedata, os, re, datetime

IN_NODES = "Data/cie10_f10_f19_nodes.csv"
IN_EDGES = "Data/cie10_f10_f19_edges_enriched.csv"
OUT_NODES = "Data/Limpieza/cie10_nodes_dedup.csv"
OUT_EDGES = "Data/Limpieza/cie10_edges_dedup.csv"
LOG_MD   = "docs/Limpieza/Entrega4_limpieza_grafos_log.md"


# -------------------- Utilidades --------------------
def strip_accents(s: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFKD", str(s)) if not unicodedata.combining(c))

def norm_text(s: str) -> str:
    s = strip_accents(str(s)).strip()
    return re.sub(r"\s+", " ", s)

def norm_cols(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [re.sub(r"[^\w]+", "_", c.lower().strip("_")) for c in df.columns]
    return df

def ensure_dir(path: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)

def pick_first(cols: list[str], candidates: list[str]) -> str | None:
    s = set(cols)
    for c in candidates:
        if c in s:
            return c
    return None

# -------------------- Limpieza --------------------
def limpiar_grafo():
    # ====== NODOS ======
    nodes = pd.read_csv(IN_NODES, sep=None, engine="python", encoding="utf-8")
    nodes = norm_cols(nodes)

    # Mapear id de nodo y campos comunes
    node_id_col = pick_first(
        list(nodes.columns),
        ["code","id","cie10_code","cie10","codigo","node","nodo"]
    )
    if node_id_col is None:
        raise ValueError("No se encontró columna de ID en nodes (ej. code/id/cie10_code).")

    # Normalizar texto
    for c in nodes.columns:
        nodes[c] = nodes[c].astype(str).apply(norm_text)

    before_nodes = len(nodes)
    nodes = nodes.drop_duplicates(subset=[node_id_col])
    after_nodes = len(nodes)

    # ====== ARISTAS ======
    edges = pd.read_csv(IN_EDGES, sep=None, engine="python", encoding="utf-8")
    edges = norm_cols(edges)

    # Detectar columnas base
    source_col = pick_first(list(edges.columns), ["source","src","from","origen","source_code","codigo_origen"])
    target_col = pick_first(list(edges.columns), ["target","dst","to","destino","target_code","codigo_destino"])
    rel_col    = pick_first(list(edges.columns), ["rel_type","relation","type","edge_type","relacion","label","tipo"])
    weight_col = pick_first(list(edges.columns), ["weight","peso","w","score","valor","strength"])

    # Validar source/target
    if source_col is None or target_col is None:
        raise ValueError(
            "No se encontraron columnas source/target en edges. "
            "Busca columnas tipo: source/src/from y target/dst/to."
        )

    # Si no hay rel_type, crear una por defecto
    if rel_col is None:
        edges["rel_type"] = "UNSPECIFIED"
        rel_col = "rel_type"

    # Normalizar texto
    for c in edges.columns:
        edges[c] = edges[c].astype(str).apply(norm_text)

    before_edges = len(edges)

    # Dedupe con las columnas disponibles
    subset_cols = [source_col, target_col]
    if rel_col in edges.columns:
        subset_cols.append(rel_col)
    edges = edges.drop_duplicates(subset=subset_cols, keep="first")
    after_edges = len(edges)

    # ====== Consistencia: source/target deben existir en nodes ======
    valid_codes = set(nodes[node_id_col])
    mask_valid = edges[source_col].isin(valid_codes) & edges[target_col].isin(valid_codes)
    bad_edges = edges[~mask_valid].copy()
    edges = edges[mask_valid].copy()

    # ====== Renombrar a esquema estándar de salida ======
    # Queremos: nodes => code, (opcional: descripcion)
    #           edges => source, target, rel_type, (opcional: weight)
    nodes_out = nodes.copy()
    if node_id_col != "code":
        nodes_out = nodes_out.rename(columns={node_id_col: "code"})

    # intuir una posible descripción
    desc_col = pick_first(list(nodes_out.columns), ["descripcion","description","label","name","titulo"])
    keep_node_cols = ["code"] + ([desc_col] if desc_col else [])
    nodes_out = nodes_out[keep_node_cols].drop_duplicates(subset=["code"])

    edges_out = edges.copy()
    rename_map = {}
    if source_col != "source": rename_map[source_col] = "source"
    if target_col != "target": rename_map[target_col] = "target"
    if rel_col    != "rel_type": rename_map[rel_col] = "rel_type"
    if weight_col and weight_col != "weight": rename_map[weight_col] = "weight"
    edges_out = edges_out.rename(columns=rename_map)

    keep_edge_cols = ["source","target","rel_type"] + (["weight"] if "weight" in edges_out.columns else [])
    edges_out = edges_out[keep_edge_cols]

    # ====== Guardar ======
    nodes_out.to_csv(OUT_NODES, index=False, encoding="utf-8")
    edges_out.to_csv(OUT_EDGES, index=False, encoding="utf-8")

    ensure_dir(LOG_MD)
    with open(LOG_MD, "w", encoding="utf-8") as f:
        f.write("# Limpieza de grafos CIE-10 (robusta)\n\n")
        f.write(f"_Generado: {datetime.datetime.now().isoformat(timespec='seconds')}_\n\n")

        f.write("## Nodos\n")
        f.write(f"- Archivo original: `{IN_NODES}`\n")
        f.write(f"- ID de nodo detectado: `{node_id_col}` → renombrado como `code`\n")
        f.write(f"- Filas antes → después: {before_nodes} → {after_nodes}\n\n")

        f.write("## Aristas\n")
        f.write(f"- Archivo original: `{IN_EDGES}`\n")
        f.write(f"- Columnas detectadas: source=`{source_col}`, target=`{target_col}`, rel_type=`{rel_col}`\n")
        if weight_col:
            f.write(f"- Columna de peso detectada: `{weight_col}`\n")
        f.write(f"- Filas antes → después (dedupe): {before_edges} → {after_edges}\n")
        f.write(f"- Aristas inválidas eliminadas (nodo inexistente): {len(bad_edges)}\n\n")
        if len(bad_edges):
            f.write("### Ejemplos de aristas inválidas:\n")
            try:
                f.write(bad_edges.head(10).to_markdown(index=False))
            except Exception:
                f.write(bad_edges.head(10).to_string(index=False))

    print("[OK] Limpieza de grafos terminada.")
    print(f" - Nodos: {before_nodes} → {after_nodes} (id='{node_id_col}')")
    print(f" - Aristas: {before_edges} → {after_edges} (rel_type='{rel_col}')")
    print(f" - Aristas inválidas eliminadas: {len(bad_edges)}")
    print(f" - Salidas: {OUT_NODES} / {OUT_EDGES}")
    print(f" - Log: {LOG_MD}")

if __name__ == "__main__":
    limpiar_grafo()
