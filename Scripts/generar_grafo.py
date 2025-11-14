#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd
import networkx as nx
import os
import pickle

# === RUTAS ===
NODES_PATH = "Data/cie10_f10_f19_nodes.csv"
EDGES_PATH = "Data/cie10_f10_f19_edges_enriched.csv"
OUTPUT_PATH = "docs/grafo_comorbilidad.gpickle"

# === CARGA ===
df_nodes = pd.read_csv(NODES_PATH)
df_edges = pd.read_csv(EDGES_PATH)

print(f"Nodos: {len(df_nodes)} | Aristas: {len(df_edges)}")

# === CREAR GRAFO ===
G = nx.from_pandas_edgelist(
    df_edges,
    source="source",
    target="target",
    edge_attr=True,
    create_using=nx.Graph()
)

# Agregar atributos de los nodos si existen
if "cie10_code" in df_nodes.columns:
    nx.set_node_attributes(G, df_nodes.set_index("cie10_code").to_dict(orient="index"))

# === GUARDAR CON PICKLE ===
os.makedirs("docs", exist_ok=True)
with open(OUTPUT_PATH, "wb") as f:
    pickle.dump(G, f)

print(f"Grafo guardado correctamente en {OUTPUT_PATH}")
print(f"Contiene {len(G.nodes())} nodos y {len(G.edges())} aristas.")
