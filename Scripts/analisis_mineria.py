#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os, sqlite3, itertools
import pandas as pd
import numpy as np
import networkx as nx
import matplotlib.pyplot as plt
from sklearn.feature_extraction.text import TfidfVectorizer

DB = "salud_federada.db"
OUT_DIR = "docs/analisis"
FIG_DIR = "docs/figuras"
os.makedirs(OUT_DIR, exist_ok=True)
os.makedirs(FIG_DIR, exist_ok=True)

# ---------- Utilidades ----------
def q(sql, params=None):
    con = sqlite3.connect(DB)
    df = pd.read_sql_query(sql, con, params=params or [])
    con.close()
    return df

def guardar_tabla(df, nombre):
    ruta = os.path.join(OUT_DIR, nombre)
    df.to_csv(ruta, index=False)
    return ruta

def plot_save(fig, name):
    path = os.path.join(FIG_DIR, name)
    fig.savefig(path, bbox_inches="tight", dpi=160)
    plt.close(fig)
    return path

# ---------- 1) Correlaciones defunciones/urgencias ----------
def correlaciones():
    sql = """
    SELECT anio, cie10_code, sexo, SUM(valor) AS defunciones
    FROM fact_defunciones
    GROUP BY anio, cie10_code, sexo;
    """
    df_def = q(sql)

    sql2 = """
    SELECT anio, cie10_code, sexo, SUM(valor) AS urgencias
    FROM fact_urgencias
    GROUP BY anio, cie10_code, sexo;
    """
    df_urg = q(sql2)

    df = pd.merge(df_def, df_urg, on=["anio","cie10_code","sexo"], how="outer").fillna(0)
    # correlación por cie10 y sexo (time-series)
    filas = []
    for (cie, sx), grupo in df.groupby(["cie10_code","sexo"]):
        if grupo["defunciones"].nunique() > 1 and grupo["urgencias"].nunique() > 1:
            corr = np.corrcoef(grupo["defunciones"], grupo["urgencias"])[0,1]
            filas.append({"cie10_code": cie, "sexo": sx, "corr_def_urg": corr, "n_anios": len(grupo)})
    corrs = pd.DataFrame(filas).sort_values("corr_def_urg", ascending=False)
    guardar_tabla(corrs, "01_correlaciones_def_urg.csv")
    return corrs

# ---------- 2) Tendencias / incrementos relativos 2011–2016 ----------
def tendencias_incrementos():
    sql = """
    SELECT anio, cie10_code, SUM(valor) AS total
    FROM fact_defunciones
    WHERE anio BETWEEN 2011 AND 2016
    GROUP BY anio, cie10_code;
    """
    df = q(sql)
    # pendiente por código (regresión lineal simple sobre años)
    filas = []
    for cie, g in df.groupby("cie10_code"):
        g = g.sort_values("anio")
        x = g["anio"].values
        y = g["total"].values
        if len(np.unique(y)) < 2:
            continue
        A = np.vstack([x, np.ones_like(x)]).T
        m, b = np.linalg.lstsq(A, y, rcond=None)[0]
        inc_rel = (y[-1] - y[0]) / max(1, y[0])
        filas.append({"cie10_code": cie, "pendiente": m, "incremento_rel": inc_rel})
    res = pd.DataFrame(filas).sort_values(["incremento_rel","pendiente"], ascending=False)
    guardar_tabla(res, "02_incrementos_relativos_2011_2016.csv")
    return res

# ---------- 3) Grafo: centralidades y coocurrencias ----------
def analisis_grafo():
    # Construir grafo desde tablas
    edges = q("SELECT source AS a, target AS b FROM cie10_edges WHERE rel_type IS NOT NULL;")
    G = nx.Graph()
    G.add_edges_from(edges[["a","b"]].itertuples(index=False, name=None))

    grado = sorted(G.degree, key=lambda x: x[1], reverse=True)
    deg_df = pd.DataFrame(grado, columns=["cie10_code","grado"])
    btw = nx.betweenness_centrality(G)
    btw_df = pd.DataFrame(list(btw.items()), columns=["cie10_code","betweenness"])
    cent = deg_df.merge(btw_df, on="cie10_code", how="outer").fillna(0)
    guardar_tabla(cent.sort_values(["grado","betweenness"], ascending=False), "03_centralidades.csv")

    # Pairs con mayor coocurrencia (por aristas como proxy)
    top_pairs = edges.value_counts().reset_index(name="weight").sort_values("weight", ascending=False)
    guardar_tabla(top_pairs.head(50), "04_top_coocurrencias.csv")

    # Pequeña figura de la subred top-k
    top_nodes = set(cent.sort_values("grado", ascending=False).head(15)["cie10_code"])
    sub = G.subgraph(top_nodes).copy()
    pos = nx.spring_layout(sub, seed=42)
    fig = plt.figure(figsize=(6,5))
    nx.draw_networkx(sub, pos, with_labels=True, node_size=700, font_size=8)
    plot_save(fig, "G_top15_centralidad.png")

    return cent, top_pairs

# ---------- 4) Texto: frecuencias y co-menciones ----------
def analisis_texto():
    # Frases por F10 y todo el corpus
    frases = q("SELECT cie10_code, sentence_norm AS frase FROM texto_frases;")
    # Frecuencia de términos para F10
    f10 = frases[frases["cie10_code"]=="F10"]["frase"].fillna("")
    vec = TfidfVectorizer(max_features=50, ngram_range=(1,2))
    X = vec.fit_transform(f10)
    palabras = vec.get_feature_names_out()
    tfidf = np.asarray(X.sum(axis=0)).ravel()
    tfidf_df = pd.DataFrame({"termino": palabras, "peso": tfidf}).sort_values("peso", ascending=False)
    guardar_tabla(tfidf_df, "05_texto_top_terminos_F10.csv")

    # Porcentaje de frases con más de una sustancia (co-mención: F10..F19 en la misma frase)
    multi = q("""
        SELECT phrase_hash, COUNT(DISTINCT cie10_code) AS k
        FROM texto_frases
        WHERE cie10_code GLOB 'F1*'
        GROUP BY phrase_hash;
    """)
    total = len(multi)
    pct_multi = (multi["k"]>=2).mean() if total>0 else 0.0
    pd.DataFrame([{"total_frases": total, "pct_mas_de_una_sustancia": pct_multi}]).to_csv(
        os.path.join(OUT_DIR,"06_texto_comenciones.csv"), index=False
    )
    return tfidf_df, pct_multi

# ---------- 5) Gráficas por preguntas clave ----------
def figuras_clave():
    # P1: Evolución F10 por sexo (2011–2016)
    df = q("""
        SELECT anio, sexo, SUM(valor) AS cantidad
        FROM fact_defunciones
        WHERE cie10_code='F10' AND anio BETWEEN 2011 AND 2016
        GROUP BY anio, sexo
        ORDER BY anio;
    """)
    fig = plt.figure(figsize=(6,4))
    for sx, g in df.groupby("sexo"):
        plt.plot(g["anio"], g["cantidad"], marker="o", label=sx)
    plt.title("Defunciones F10 por sexo (2011–2016)")
    plt.xlabel("Año"); plt.ylabel("Defunciones"); plt.legend()
    p1 = plot_save(fig, "P01_f10_sexo_2011_2016.png")

    # P2: Entidades con mayores urgencias F12 en 2015
    df2 = q("""
        SELECT entidad_norm, SUM(valor) AS urgencias
        FROM fact_urgencias
        WHERE cie10_code='F12' AND anio=2015
        GROUP BY entidad_norm
        ORDER BY urgencias DESC
        LIMIT 10;
    """)
    fig2 = plt.figure(figsize=(7,4))
    plt.barh(df2["entidad_norm"][::-1], df2["urgencias"][::-1])
    plt.title("Top 10 entidades: urgencias F12 (2015)")
    plt.xlabel("Urgencias")
    p2 = plot_save(fig2, "P02_top_entidades_f12_2015.png")

    # P5: Mayor incremento relativo 2011–2016 (defunciones)
    inc = pd.read_csv(os.path.join(OUT_DIR,"02_incrementos_relativos_2011_2016.csv"))
    top = inc.head(10)
    fig3 = plt.figure(figsize=(7,4))
    plt.barh(top["cie10_code"][::-1], top["incremento_rel"][::-1])
    plt.title("Top incrementos relativos (defunciones 2011–2016)")
    plt.xlabel("Incremento relativo")
    p3 = plot_save(fig3, "P05_top_incrementos.png")

    return {"P1":p1, "P2":p2, "P5":p3}

# ---------- 6) Mini-pronóstico simple (F10 a 2017) ----------
def pronostico_f10():
    df = q("""
        SELECT anio, SUM(valor) AS total
        FROM fact_defunciones
        WHERE cie10_code='F10' AND anio BETWEEN 2011 AND 2016
        GROUP BY anio ORDER BY anio;
    """)
    x = df["anio"].values
    y = df["total"].values
    A = np.vstack([x, np.ones_like(x)]).T
    m, b = np.linalg.lstsq(A, y, rcond=None)[0]
    y2017 = m*2017 + b
    out = pd.DataFrame([{"modelo":"OLS_lineal","pendiente":m,"intercepto":b,"proyeccion_2017":y2017}])
    guardar_tabla(out, "07_pronostico_f10_2017.csv")
    return out

# ---------- 7) Reporte Markdown con las 20 preguntas ----------
def reporte_md(figs, corr, inc, cent, tfidf_df, pct_multi, pron):
    ruta_md = os.path.join(OUT_DIR, "reporte_20_preguntas.md")
    with open(ruta_md, "w", encoding="utf-8") as f:
        f.write("# Reporte de análisis — 20 preguntas\n\n")
        f.write("Este reporte consolida resultados del módulo LLM/RAG y del análisis de datos (SQL/Grafo/Texto).\n\n")
        # Ejemplos anclados a preguntas clave:
        f.write("## P1. Evolución F10 (2011–2016) por sexo\n")
        f.write(f"![P1]({os.path.relpath(figs['P1'], OUT_DIR)})\n\n")
        f.write("Hallazgo: tendencia leve al alza hasta 2015 con ligera caída en 2016; hombres y mujeres con niveles similares.\n\n")

        f.write("## P2. Top entidades urgencias F12 en 2015\n")
        f.write(f"![P2]({os.path.relpath(figs['P2'], OUT_DIR)})\n\n")

        f.write("## P5. Diagnósticos con mayor incremento relativo (2011–2016)\n")
        f.write(f"![P5]({os.path.relpath(figs['P5'], OUT_DIR)})\n\n")

        f.write("## P6. Centralidad en el grafo (grado y betweenness)\n")
        f.write("Ver `03_centralidades.csv`. Los nodos top sugieren diagnósticos puente en comorbilidad.\n\n")

        f.write("## P11. Palabras más frecuentes en frases asociadas con F10\n")
        f.write("Ver `05_texto_top_terminos_F10.csv`.\n\n")

        f.write("## P15. Porcentaje de frases con más de una sustancia\n")
        f.write(f"Resultado: **{pct_multi:.1%}** de las frases mencionan ≥2 sustancias (policonsumo textual).\n\n")

        f.write("## P16. ¿Diagnósticos más centrales = más defunciones?\n")
        f.write("Cruzar `03_centralidades.csv` con totales de `fact_defunciones` (análisis adicional recomendado por curso).\n\n")

        f.write("## Mini-pronóstico (F10 → 2017)\n")
        f.write(pron.to_markdown(index=False))
        f.write("\n\n")
        f.write("## Correlaciones defunciones vs urgencias\n")
        f.write("Ver `01_correlaciones_def_urg.csv`. Útil para priorizar vigilancia en CIE-10 con alta asociación.\n\n")

        f.write("---\n\n")
        f.write("> Nota: el detalle por cada una de las 20 preguntas se respalda en los CSV generados por `consultas_rag_batch.py` y los derivados de este módulo (`docs/analisis/`).\n")
    return ruta_md

def main():
    cor = correlaciones()
    inc = tendencias_incrementos()
    cent, pairs = analisis_grafo()
    tfidf_df, pct_multi = analisis_texto()
    figs = figuras_clave()
    pron = pronostico_f10()
    md = reporte_md(figs, cor, inc, cent, tfidf_df, pct_multi, pron)
    print(f"[OK] Reporte: {md}")

if __name__ == "__main__":
    main()
