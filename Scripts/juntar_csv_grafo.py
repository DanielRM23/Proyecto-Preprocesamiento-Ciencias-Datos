import pandas as pd
import networkx as nx
from pathlib import Path

DATA = Path("./Data")

# 1) Cargar datos tidy (formato largo) y catálogo de descripciones
tidy = pd.read_csv(DATA/"psa_tidy_long.csv")  # columnas clave: anio_defuncion, entidad_defuncion, code, valor, fuente, ...
cat  = pd.read_csv(DATA/"cie10_f10_f19_roots.csv")  # code, descripcion

# 2) Cargar grafo enriquecido (jerarquía + relaciones clínicas)
edges_en = pd.read_csv(DATA/"cie10_f10_f19_edges_enriched.csv")
G = nx.from_pandas_edgelist(
    edges_en, source="source", target="target", edge_attr=True, create_using=nx.DiGraph
)

# Utilidades
def subtipos(code_root: str):
    """Devuelve subtipos .0.. .9 de un root (F10 -> F10.0..F10.9) usando aristas jerárquicas."""
    return [v for u, v, d in G.edges(data=True)
            if u == code_root and d.get("relation") == "is_a/subtype_of"]

def relacionados_clinicos(code_root: str):
    """Devuelve raíces relacionados clínicamente (no jerárquicos)."""
    return [v for u, v, d in G.edges(data=True)
            if u == code_root and d.get("relation") != "is_a/subtype_of" and "." not in v]

def expandir_codigo(code_root: str, incluir_subtipos=True, incluir_clinicos=True):
    """Conjunto de códigos a consultar (root + opcionales)."""
    codes = {code_root}
    if incluir_subtipos:
        codes.update(subtipos(code_root))
    if incluir_clinicos:
        codes.update(relacionados_clinicos(code_root))
        # opcional: también agregar subtipos de los relacionados clínicos
        for r in list(codes):
            if len(r) == 3 and r.startswith("F"):  # es raíz tipo F10
                codes.update(subtipos(r))
    return sorted(codes)

# ====== EJEMPLO DE INTEGRACIÓN ======
# Caso: analizar F10 (alcohol) + sus relacionados clínicos (tabaco F17, cocaína F14, cannabis F12) y subtipos
codes_consulta = expandir_codigo("F10", incluir_subtipos=True, incluir_clinicos=True)
print("Códigos consultados:", codes_consulta)

# Filtrar tidy a esos códigos y agregar por año/entidad
subset = tidy[tidy["code"].isin(codes_consulta)].copy()

# Agregado total por código (defunciones+urgencias)
totales_por_codigo = (subset.groupby("code", as_index=False)["valor"].sum()
                      .merge(cat, on="code", how="left")
                      .sort_values("valor", ascending=False))

print("\nTotales por código (con descripción):\n", totales_por_codigo.head(15))

# Agregado por año y código (útil para series temporales)
por_anio_codigo = (subset.groupby(["anio_defuncion","code"], as_index=False)["valor"].sum()
                   .merge(cat, on="code", how="left")
                   .sort_values(["anio_defuncion","valor"], ascending=[True, False]))

print("\nPor año y código (top 10 filas):\n", por_anio_codigo.head(10))

# Agregado por entidad (geográfico) y código
por_entidad_codigo = (subset.groupby(["entidad_defuncion","code"], as_index=False)["valor"].sum()
                      .merge(cat, on="code", how="left")
                      .sort_values(["entidad_defuncion","valor"], ascending=[True, False]))

print("\nPor entidad y código (top 10 filas):\n", por_entidad_codigo.head(10))

import pandas as pd
import networkx as nx
from pathlib import Path

DATA = Path("./Data")

# 1) Cargar datos tidy (formato largo) y catálogo de descripciones
tidy = pd.read_csv(DATA/"psa_tidy_long.csv")  # columnas clave: anio_defuncion, entidad_defuncion, code, valor, fuente, ...
cat  = pd.read_csv(DATA/"cie10_f10_f19_roots.csv")  # code, descripcion

# 2) Cargar grafo enriquecido (jerarquía + relaciones clínicas)
edges_en = pd.read_csv(DATA/"cie10_f10_f19_edges_enriched.csv")
G = nx.from_pandas_edgelist(
    edges_en, source="source", target="target", edge_attr=True, create_using=nx.DiGraph
)

# Utilidades
def subtipos(code_root: str):
    """Devuelve subtipos .0.. .9 de un root (F10 -> F10.0..F10.9) usando aristas jerárquicas."""
    return [v for u, v, d in G.edges(data=True)
            if u == code_root and d.get("relation") == "is_a/subtype_of"]

def relacionados_clinicos(code_root: str):
    """Devuelve raíces relacionados clínicamente (no jerárquicos)."""
    return [v for u, v, d in G.edges(data=True)
            if u == code_root and d.get("relation") != "is_a/subtype_of" and "." not in v]

def expandir_codigo(code_root: str, incluir_subtipos=True, incluir_clinicos=True):
    """Conjunto de códigos a consultar (root + opcionales)."""
    codes = {code_root}
    if incluir_subtipos:
        codes.update(subtipos(code_root))
    if incluir_clinicos:
        codes.update(relacionados_clinicos(code_root))
        # opcional: también agregar subtipos de los relacionados clínicos
        for r in list(codes):
            if len(r) == 3 and r.startswith("F"):  # es raíz tipo F10
                codes.update(subtipos(r))
    return sorted(codes)

# ====== EJEMPLO DE INTEGRACIÓN ======
# Caso: analizar F10 (alcohol) + sus relacionados clínicos (tabaco F17, cocaína F14, cannabis F12) y subtipos
codes_consulta = expandir_codigo("F10", incluir_subtipos=True, incluir_clinicos=True)
print("Códigos consultados:", codes_consulta)

# Filtrar tidy a esos códigos y agregar por año/entidad
subset = tidy[tidy["code"].isin(codes_consulta)].copy()

# Agregado total por código (defunciones+urgencias)
totales_por_codigo = (subset.groupby("code", as_index=False)["valor"].sum()
                      .merge(cat, on="code", how="left")
                      .sort_values("valor", ascending=False))

print("\nTotales por código (con descripción):\n", totales_por_codigo.head(15))

# Agregado por año y código (útil para series temporales)
por_anio_codigo = (subset.groupby(["anio_defuncion","code"], as_index=False)["valor"].sum()
                   .merge(cat, on="code", how="left")
                   .sort_values(["anio_defuncion","valor"], ascending=[True, False]))

print("\nPor año y código (top 10 filas):\n", por_anio_codigo.head(10))

# Agregado por entidad (geográfico) y código
por_entidad_codigo = (subset.groupby(["entidad_defuncion","code"], as_index=False)["valor"].sum()
                      .merge(cat, on="code", how="left")
                      .sort_values(["entidad_defuncion","valor"], ascending=[True, False]))

print("\nPor entidad y código (top 10 filas):\n", por_entidad_codigo.head(10))


# Guardar resultados como CSV
totales_por_codigo.to_csv(DATA/"totales_por_codigo.csv", index=False)
por_anio_codigo.to_csv(DATA/"por_anio_codigo.csv", index=False)
por_entidad_codigo.to_csv(DATA/"por_entidad_codigo.csv", index=False)

print("Archivos guardados en ./Data/")

# Guardar resultados como CSV
totales_por_codigo.to_csv(DATA/"totales_por_codigo.csv", index=False)
por_anio_codigo.to_csv(DATA/"por_anio_codigo.csv", index=False)
por_entidad_codigo.to_csv(DATA/"por_entidad_codigo.csv", index=False)

print("Archivos guardados en ./Data/")
