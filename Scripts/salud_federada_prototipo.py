#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Prototipo: Base de datos heterogénea federada (Salud, español)
- Relacional (CSV -> SQL): defunciones, urgencias
- Grafo (NetworkX): CIE-10 F10..F19 (nodos/aristas)
- Texto (mini-NLP): mapeo simple palabra clave -> código CIE-10
Uso:
    # Opción 1 (Postgres): export DATABASE_URL='postgresql+psycopg2://usuario:password@localhost:5432/salud_federada'
    # Opción 2 (demo SQLite): no exportes nada y usará 'sqlite:///salud_federada.db' en el cwd
    python salud_federada_prototipo.py --data-dir ./Data
Requisitos:
    pip install pandas sqlalchemy psycopg2-binary networkx python-dotenv
    # opcional: spacy es_core_news_sm
"""
import argparse
import os
from pathlib import Path
import pandas as pd
import numpy as np
import networkx as nx
from sqlalchemy import create_engine, text

# ==========================
# Configuración de conexión
# ==========================
def get_engine():
    db_url = os.environ.get("DATABASE_URL", "").strip()
    if not db_url:
        # Fallback a SQLite para que el prototipo funcione sin Postgres
        db_url = "sqlite:///salud_federada.db"
        print(f"[INFO] DATABASE_URL no definido. Usando fallback local: {db_url}")
    else:
        print(f"[INFO] Usando DATABASE_URL: {db_url}")
    return create_engine(db_url, future=True)

# ==========================
# Utilidades
# ==========================
def normalizar_entidad(col):
    """Normaliza nombres/códigos de entidad a mayúsculas y sin tildes simples (placeholder)."""
    # Puedes extender esto con un catálogo INEGI.
    return (col.astype(str)
               .str.strip()
               .str.upper()
               .str.replace(r"\s+", " ", regex=True))

def columnas_esperadas_def():
    return ['anio_defuncion', 'entidad_defuncion', 'edad_quinquenal', 'sexo',
            'F10','F11','F12','F14','F15','F17','F18','F19','F13','F16',
            'cve_entidad','fecha','entidad_defuncion_etq']

def columnas_esperadas_urg():
    return ['anio','entidad','edad_quinquenal','sexo','F10','F11','F12','F13','F14','F15','F16','F17','F18','F19','fecha']

def verificar_columnas(df, esperadas, nombre):
    faltantes = [c for c in esperadas if c not in df.columns]
    if faltantes:
        raise ValueError(f"[ERROR] {nombre}: faltan columnas {faltantes}")
    # Deja pasar columnas extra; solo aseguramos las claves mínimas y F10..F19.

# ==========================
# Carga Relacional (CSV -> SQL)
# ==========================
def cargar_tablas_relacionales(engine, data_dir: Path):
    # Rutas
    f_def = data_dir / "defunciones_uso_sustancias_clean.csv"
    f_urg = data_dir / "urgencias_uso_sustancias_clean.csv"
    if not f_def.exists():
        raise FileNotFoundError(f"No se encontró {f_def}")
    if not f_urg.exists():
        raise FileNotFoundError(f"No se encontró {f_urg}")

    # Leer
    df_def = pd.read_csv(f_def)
    df_urg = pd.read_csv(f_urg)

    # Verificar esquemas mínimos
    verificar_columnas(df_def, columnas_esperadas_def(), "defunciones")
    verificar_columnas(df_urg, columnas_esperadas_urg(), "urgencias")

    # Normalizar claves de unión
    df_def['anio'] = df_def['anio_defuncion'].astype(int)
    df_def['entidad_norm'] = normalizar_entidad(df_def['entidad_defuncion_etq'].fillna(df_def['entidad_defuncion']))
    df_urg['entidad_norm'] = normalizar_entidad(df_urg['entidad'])

    # Índices útiles (opcional)
    # df_def.set_index(['anio','entidad_norm','sexo','edad_quinquenal'], inplace=False)
    # df_urg.set_index(['anio','entidad_norm','sexo','edad_quinquenal'], inplace=False)

    # Guardar a SQL
    df_def.to_sql("defunciones", engine, if_exists="replace", index=False)
    df_urg.to_sql("urgencias", engine, if_exists="replace", index=False)

    print(f"[OK] Cargadas tablas relacionales: defunciones ({len(df_def)}) y urgencias ({len(df_urg)})")

# ==========================
# Carga Grafo (NetworkX)
# ==========================
def construir_grafo_cie10(data_dir: Path):
    f_nodes = data_dir / "cie10_f10_f19_nodes.csv"
    f_edges = data_dir / "cie10_f10_f19_edges_enriched.csv"
    if not f_nodes.exists():
        raise FileNotFoundError(f"No se encontró {f_nodes}")
    if not f_edges.exists():
        raise FileNotFoundError(f"No se encontró {f_edges}")

    nodes = pd.read_csv(f_nodes)
    edges = pd.read_csv(f_edges)

    # Grafo dirigido
    G = nx.from_pandas_edgelist(
        edges, source="source", target="target", edge_attr=True, create_using=nx.DiGraph
    )

    # Cargar atributos de nodos si existen
    node_id_col = None
    for candidate in ["code","id","node","source"]:
        if candidate in nodes.columns:
            node_id_col = candidate
            break
    if node_id_col is None:
        raise ValueError("[ERROR] No se encontró una columna identificadora de nodo en nodes CSV (esperado: code/id/node)")

    for _, row in nodes.iterrows():
        nid = str(row[node_id_col])
        for c in nodes.columns:
            if c == node_id_col: 
                continue
            G.nodes[nid][c] = row[c]

    print(f"[OK] Grafo CIE-10 creado: {G.number_of_nodes()} nodos, {G.number_of_edges()} aristas")
    return G

def subtipos_directos(G: nx.DiGraph, code: str):
    """Hijos inmediatos (aristas code -> hijo)."""
    code = str(code)
    return list(G.successors(code)) if code in G else []

def ancestros(G: nx.DiGraph, code: str):
    code = str(code)
    return list(nx.ancestors(G, code)) if code in G else []

# ==========================
# Texto / Mini NLP (prototipo)
# ==========================
SINONIMOS_A_CIE10 = {
    # mapeo muy simple de palabras clave a códigos CIE-10 (puedes extenderlo)
    "alcohol": "F10",
    "cocaína": "F14",
    "cocaina": "F14",
    "opioides": "F11",
    "opiode": "F11",
    "cannabis": "F12",
    "sedantes": "F13",
    "hipnóticos": "F13",
    "hipnoticos": "F13",
    "anfetaminas": "F15",
    "tabaco": "F17",
    "solventes": "F18",
    "alucinógenos": "F16",
    "alucinogenos": "F16",
    "múltiples": "F19",
    "multiples": "F19",
}

def mapear_texto_a_cie10(texto: str):
    t = texto.lower()
    for clave, code in SINONIMOS_A_CIE10.items():
        if clave in t:
            return code
    return None

# ==========================
# Consultas ejemplo (federadas en espíritu)
# ==========================
def consulta_sql_resumen(engine, code="F10", anio_min=None, entidad_norm=None, tabla="defunciones"):
    """Agrega por año/entidad para un código Fxx específico. Compatible con SQLite y Postgres."""
    code = code.upper()
    if code not in [f"F{i}" for i in [10,11,12,13,14,15,16,17,18,19]]:
        raise ValueError("Código no soportado en este prototipo (F10..F19)")
    col_code = code  # las columnas en CSV ya vienen como F10..F19
    where = []
    if anio_min is not None:
        # En defunciones el año está en 'anio' (creado), en urgencias el año es 'anio'
        where.append("anio >= :anio_min")
    if entidad_norm is not None:
        where.append("entidad_norm = :entidad_norm")
    where_sql = ("WHERE " + " AND ".join(where)) if where else ""
    sql = f"""
        SELECT anio, entidad_norm, SUM(COALESCE({col_code},0)) AS total_{code}
        FROM {tabla}
        {where_sql}
        GROUP BY anio, entidad_norm
        ORDER BY anio, entidad_norm;
    """
    params = {"anio_min": anio_min, "entidad_norm": entidad_norm}
    params = {k:v for k,v in params.items() if v is not None}
    with engine.begin() as conn:
        return pd.read_sql(text(sql), conn, params=params)

def ejemplo_consulta_federada(engine, G, texto_libre, anio_min=2015, entidad_norm=None, tabla="defunciones"):
    """
    Dado un texto (ej. 'intoxicación por alcohol'), mapea a CIE-10 y:
      1) Pregunta a SQL cuántos casos hay por año/entidad para ese código
      2) Recupera subtipos directos en el grafo para exploración clínica
    """
    code = mapear_texto_a_cie10(texto_libre)
    if not code:
        return {"code": None, "sql": pd.DataFrame(), "subtipos": []}
    df_sql = consulta_sql_resumen(engine, code=code, anio_min=anio_min, entidad_norm=entidad_norm, tabla=tabla)
    hijos = subtipos_directos(G, code)
    return {"code": code, "sql": df_sql, "subtipos": hijos}

# ==========================
# Main
# ==========================
def main():
    parser = argparse.ArgumentParser(description="Prototipo BD heterogénea federada (Salud).")
    parser.add_argument("--data-dir", type=str, required=True, help="Carpeta que contiene los CSV fuente.")
    parser.add_argument("--tabla", type=str, choices=["defunciones","urgencias"], default="defunciones",
                        help="Tabla a consultar en ejemplo federado.")
    parser.add_argument("--texto", type=str, default="Urgencia por consumo de alcohol crónico",
                        help="Texto libre para demo de mapeo -> CIE-10.")
    parser.add_argument("--anio-min", type=int, default=2015)
    parser.add_argument("--entidad", type=str, default=None, help="Filtrar por entidad (etiqueta normalizada).")
    args = parser.parse_args()

    data_dir = Path(args.data_dir).resolve()
    if not data_dir.exists():
        raise SystemExit(f"[ERROR] data-dir no existe: {data_dir}")

    engine = get_engine()

    # 1) Cargar relacional
    cargar_tablas_relacionales(engine, data_dir)

    # 2) Construir grafo
    G = construir_grafo_cie10(data_dir)

    # 3) Consulta demo SQL
    print("\n=== Consulta SQL ejemplo (F10 en defunciones, desde anio_min) ===")
    df_ej = consulta_sql_resumen(engine, code="F10", anio_min=args.anio_min, entidad_norm=args.entidad, tabla=args.tabla)
    print(df_ej.head(10))

    # 4) Consulta federada (texto -> CIE-10 -> SQL + Grafo)
    print("\n=== Consulta federada (texto -> CIE-10 -> SQL + subtipos grafo) ===")
    res = ejemplo_consulta_federada(engine, G, texto_libre=args.texto, anio_min=args.anio_min, entidad_norm=args.entidad, tabla=args.tabla)
    if res["code"] is None:
        print(f"No se encontró mapeo CIE-10 para el texto: '{args.texto}'")
    else:
        print(f"Texto: '{args.texto}' -> Código CIE-10: {res['code']}")
        print("Top 10 filas SQL:")
        print(res["sql"].head(10))
        print("Subtipos directos en grafo:", res["subtipos"])

    print("\n[LISTO] Prueba ejecutada correctamente.")

if __name__ == "__main__":
    main()
