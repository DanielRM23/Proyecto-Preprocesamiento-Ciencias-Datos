#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
consultas_rag_batch.py
---------------------------------------------------------------
Versión batch híbrida para las 20 preguntas del proyecto:
- Ejecuta automáticamente cada pregunta con la mejor estrategia (SQL, grafo, texto o híbrida)
- Usa Ollama (modelo mistral)
- Guarda todas las respuestas y fragmentos en docs/llm_resultados/
"""

import os
import sqlite3
import pandas as pd
from datetime import datetime
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import ollama

# =====================================================
# CONFIGURACIÓN
# =====================================================

DB_PATH = "salud_federada.db"
OUTPUT_DIR = "docs/llm_resultados"
os.makedirs(OUTPUT_DIR, exist_ok=True)
OLLAMA_MODEL = "mistral"


# =====================================================
# FUNCIONES DE APOYO
# =====================================================

def cargar_corpus(conn, limite=6000):
    """Carga corpus textual desde v_unificado."""
    df = pd.read_sql_query("SELECT origen, cie10_code, texto FROM v_unificado LIMIT ?;", conn, params=(limite,))
    corpus = [
        f"{fila['origen']} | cie10={fila['cie10_code']} | {fila.get('texto','')}"
        for _, fila in df.iterrows()
    ]
    vectorizer = TfidfVectorizer(lowercase=True, ngram_range=(1, 2))
    X = vectorizer.fit_transform(corpus)
    return vectorizer, X, corpus


def recuperar(query, vectorizer, X, corpus, k=6):
    """Recupera los k fragmentos más relevantes."""
    qv = vectorizer.transform([query])
    sims = cosine_similarity(qv, X)[0]
    idxs = sims.argsort()[::-1][:k]
    return [corpus[i] for i in idxs]


def consulta_sql(conn, cie10, tabla, anio_ini=None, anio_fin=None):
    """Obtiene datos reales (si aplica)."""
    conds = [f"cie10_code LIKE '{cie10}%'" if cie10 else "1=1"]
    if anio_ini and anio_fin:
        conds.append(f"anio BETWEEN {anio_ini} AND {anio_fin}")
    where = " AND ".join(conds)

    query = f"SELECT * FROM {tabla} WHERE {where} LIMIT 5000;"
    try:
        return pd.read_sql_query(query, conn)
    except Exception as e:
        print(f"[WARN] Error en consulta SQL: {e}")
        return pd.DataFrame()


def generar_prompt(pregunta, evidencias, datos_sql=None):
    """Crea prompt unificado con datos y contexto."""
    ctx = "\n".join(f"- {e}" for e in evidencias)
    sql_txt = ""
    if datos_sql is not None and not datos_sql.empty:
        sql_txt = "\nDatos reales:\n" + "\n".join(str(row) for _, row in datos_sql.iterrows())

    return f"""
Eres un analista de salud pública. Usa los datos reales y el contexto textual para responder de forma precisa y concisa.
Pregunta:
{pregunta}

{sql_txt}

Contexto textual:
{ctx}

Responde en español, incluyendo patrones, comparaciones y referencias a las fuentes (sql_defunciones, sql_urgencias, grafo o texto).
"""


def ejecutar_consulta(pregunta, conn, vectorizer, X, corpus, estrategia="rag", cie10=None, tabla=None, anio_ini=None, anio_fin=None):
    """Ejecuta una consulta según el tipo (RAG o híbrida)."""
    evidencias = recuperar(pregunta, vectorizer, X, corpus)

    datos_sql = None
    if estrategia == "hibrida" and cie10 and tabla:
        datos_sql = consulta_sql(conn, cie10, tabla, anio_ini, anio_fin)

    prompt = generar_prompt(pregunta, evidencias, datos_sql)

    response = ollama.chat(model=OLLAMA_MODEL, messages=[
        {"role": "system", "content": "Eres un analista de salud pública especializado en datos epidemiológicos."},
        {"role": "user", "content": prompt}
    ])
    respuesta = response["message"]["content"].strip()

    return {"pregunta": pregunta, "respuesta": respuesta, "fragmentos": evidencias, "datos_sql": datos_sql}


def guardar_resultado(result):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    nombre = f"{timestamp}__{result['pregunta'][:60].replace(' ','_')}.csv"
    ruta = os.path.join(OUTPUT_DIR, nombre)
    pd.DataFrame({
        "pregunta": [result["pregunta"]],
        "respuesta": [result["respuesta"]],
        "fragmentos": ["\n".join(result["fragmentos"])]
    }).to_csv(ruta, index=False)
    print(f"[OK] Resultado guardado en {ruta}")

    if result["datos_sql"] is not None and not result["datos_sql"].empty:
        ruta_sql = ruta.replace(".csv", "_datos_sql.csv")
        result["datos_sql"].to_csv(ruta_sql, index=False)
        print(f"[OK] Datos reales guardados en {ruta_sql}")


# =====================================================
# PREGUNTAS Y ESTRATEGIAS
# =====================================================

PREGUNTAS_DESCRIPTIVAS = [
    # Qué es lo que tengo y que puedo ver con mis datos que ya tengo 
    # Estas salen con consultas, no necesariamente
    ("¿Cuál es la evolución temporal del número de defunciones por F10 (alcohol) entre 2011 y 2016, diferenciando por sexo?", "hibrida", "F10", "fact_defunciones", 2011, 2016),
    ("¿Qué entidades federativas presentan las mayores tasas de urgencias por F12 (cannabis) en 2015?", "hibrida", "F12", "fact_urgencias", 2015, 2015),
    ("¿Cuál es la proporción de defunciones por F14 (cocaína) respecto al total de muertes por sustancias en 2015?", "hibrida", "F14", "fact_defunciones", 2015, 2015),
    ("¿En qué grupos de edad se concentran las defunciones por F15 (estimulantes)?", "hibrida", "F15", "fact_defunciones", None, None),
    ("¿Qué diagnósticos presentan el mayor incremento relativo de casos entre 2011 y 2016?", "hibrida", None, "fact_defunciones", 2011, 2016),
    ("¿Qué palabras son más frecuentes en frases asociadas con F10 (alcohol)?", "rag", None, None, None, None),
    ("¿Qué frases del corpus mencionan simultáneamente términos asociados con dependencia y opioides?", "rag", None, None, None, None),
    ("¿Qué códigos CIE-10 aparecen más referenciados en el corpus textual?", "rag", None, None, None, None),
    ("¿Qué porcentaje de frases menciona más de una sustancia psicoactiva?", "rag", None, None, None, None),
    ("¿Qué entidades con mayor número de urgencias por F16 (alucinógenos) aparecen en frases con lenguaje de alarma o gravedad?", "hibrida", "F16", "fact_urgencias", None, None),
]

PREGUNTAS_PREDICTIVAS = [
    # Qué puedo anticipar o predecir con mis datos y modelos
    # Estas son las que tengo que sacar con el LLM 
    ("¿Qué códigos CIE-10 son los nodos más centrales del grafo de comorbilidad (grado y betweenness)?", "rag", None, None, None, None),
    ("¿Qué sustancias se encuentran más conectadas con F10 (alcohol)?", "rag", None, None, None, None),
    ("¿Existen comunidades o clústeres que agrupen sustancias similares según sus conexiones en el grafo?", "rag", None, None, None, None),
    ("¿Qué tan fuerte es la conexión entre F12 (cannabis) y F20–F29 (trastornos psicóticos)?", "rag", None, None, None, None),
    ("¿Qué pares de diagnósticos tienen la mayor coocurrencia según el grafo de comorbilidad?", "rag", None, None, None, None),
    ("¿Qué términos aparecen más próximos (semánticamente) a crack o piedra?", "rag", None, None, None, None),
    ("¿Los diagnósticos más centrales en el grafo de comorbilidad son también los que presentan mayor número de defunciones?", "rag", None, None, None, None),
    ("¿Qué pares de diagnósticos con alta coocurrencia en el grafo se mencionan juntos en el corpus textual?", "rag", None, None, None, None),
    ("¿Qué combinaciones de sustancias según el grafo se mencionan frecuentemente juntas en el corpus textual?", "rag", None, None, None, None),
    ("Según los patrones del grafo y el corpus textual, qué nuevas combinaciones de sustancias podrían representar riesgo emergente de policonsumo?", "rag", None, None, None, None),
]


PREGUNTAS = PREGUNTAS_DESCRIPTIVAS + PREGUNTAS_PREDICTIVAS


# =====================================================
# FLUJO PRINCIPAL
# =====================================================

def main():
    conn = sqlite3.connect(DB_PATH)
    vectorizer, X, corpus = cargar_corpus(conn)
    print(f"[OK] Corpus cargado y vectorizado con {len(corpus)} documentos.\n")

    for i, (pregunta, estrategia, cie10, tabla, a_ini, a_fin) in enumerate(PREGUNTAS, 1):
        print(f"\n--- Pregunta {i}/{len(PREGUNTAS)} ---")
        print(pregunta)
        result = ejecutar_consulta(pregunta, conn, vectorizer, X, corpus, estrategia, cie10, tabla, a_ini, a_fin)
        guardar_resultado(result)

    conn.close()
    print("\n[FIN] Todas las consultas completadas.")


if __name__ == "__main__":
    main()
