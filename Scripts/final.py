import pandas as pd
from sqlalchemy import create_engine, text
import networkx as nx
from pathlib import Path

# === rutas (ajusta si hace falta) ===
DATA = Path("./Data")
TEXTOS = Path("./Textos")

# === conexión: usa la misma lógica que el prototipo (SQLite por defecto) ===
DATABASE_URL = None  # o 'postgresql+psycopg2://user:pass@localhost:5432/salud_federada'
engine = create_engine(DATABASE_URL or "sqlite:///salud_federada.db", future=True)

# === 1) Cargar matches de CoWeSe (texto -> CIE-10) ===
matches = pd.read_csv(TEXTOS/"cowese_matches.csv")  # columnas: doc_id, sent_id, sentence, keyword, cie10
# Nos quedamos con F10..F19
valid_codes = {f"F{i}" for i in [10,11,12,13,14,15,16,17,18,19]}
matches = matches[matches['cie10'].isin(valid_codes)].copy()

# Frecuencia de menciones por código
freq_texto = matches['cie10'].value_counts().rename_axis('cie10').reset_index(name='menciones_texto')
print("Frecuencia de menciones en textos (Top):")
print(freq_texto.head(10))

# === 2) Agregar por año/entidad en SQL para un código dado (defunciones/urgencias) ===
def resumen_sql(code="F10", tabla="defunciones", anio_min=2015, entidad_norm=None):
    where = ["anio >= :anio_min"]
    params = {"anio_min": anio_min}
    if entidad_norm:
        where.append("entidad_norm = :entidad_norm")
        params["entidad_norm"] = entidad_norm
    where_sql = "WHERE " + " AND ".join(where)
    sql = f"""
        SELECT anio, entidad_norm, SUM(COALESCE({code},0)) AS total_{code}
        FROM {tabla}
        {where_sql}
        GROUP BY anio, entidad_norm
        ORDER BY anio, entidad_norm;
    """
    with engine.begin() as conn:
        return pd.read_sql(text(sql), conn, params=params)

# Ejemplo: resumen para F10 en defunciones desde 2015
df_def_F10 = resumen_sql("F10", "defunciones", 2015)
print("\nDefunciones por F10 (Top 5):")
print(df_def_F10.head())

# === 3) Construir el grafo (igual que tu prototipo) y explorar subtipos/relaciones ===
edges_en = pd.read_csv(DATA/"cie10_f10_f19_edges_enriched.csv")
G = nx.from_pandas_edgelist(edges_en, source="source", target="target", edge_attr=True, create_using=nx.DiGraph)

def subtipos_directos(G, code):
    return list(G.successors(code)) if code in G else []

print("\nSubtipos F10:", subtipos_directos(G, "F10"))

# === 4) Un mini “join semántico”: texto -> código -> SQL + Grafo ===
def federada_por_keyword(keyword_busqueda="alcohol", tabla="defunciones", anio_min=2015):
    # 4.1 filtrar oraciones de CoWeSe que contengan la keyword
    sub = matches[matches['sentence'].str.lower().str.contains(keyword_busqueda.lower())]
    codigos = sorted(sub['cie10'].unique().tolist())
    if not codigos:
        return {"codigos": [], "sql": {}, "subtipos": {}}

    # 4.2 para cada código, sacar resumen SQL
    sql_result = {}
    for c in codigos:
        sql_result[c] = resumen_sql(c, tabla=tabla, anio_min=anio_min)

    # 4.3 para cada código, subtipos en grafo
    subt = {c: subtipos_directos(G, c) for c in codigos}
    return {"codigos": codigos, "sql": sql_result, "subtipos": subt, "ejemplos_oraciones": sub.head(10)}

demo = federada_por_keyword("cocaína", tabla="urgencias", anio_min=2018)
print("\nCódigos detectados para 'cocaína':", demo["codigos"])
for c, df in demo["sql"].items():
    print(f"\nResumen SQL para {c} (Top 5):")
    print(df.head())
print("\nSubtipos por código:", demo["subtipos"])
print("\nEjemplos de oraciones (Top 5):")
print(demo["ejemplos_oraciones"][['keyword','cie10','sentence']].head())
