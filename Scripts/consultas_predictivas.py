#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
consultas_rag_predictivas.py
---------------------------------------------------------------
Responde AUTOMÁTICAMENTE las 10 preguntas predictivas del proyecto.

- Usa Ollama (modelo mistral)
- Usa resultados descriptivos en CSV como contexto REAL
- Usa contexto textual vía RAG (TF-IDF)
- Genera respuestas predictivas justificadas y basadas en datos

Salida:
    docs/llm_resultados_predictivas/*.csv
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
DESCR_DIR = "docs/consultas_descriptivas"
OUTPUT_DIR = "docs/llm_resultados_predictivas"
os.makedirs(OUTPUT_DIR, exist_ok=True)

OLLAMA_MODEL = "mistral"


# =====================================================
# FUNCIONES DE APOYO
# =====================================================

def cargar_corpus(conn, limite=8000):
    """Carga corpus textual desde v_unificado."""
    df = pd.read_sql_query(
        "SELECT origen, cie10_code, texto FROM v_unificado WHERE texto IS NOT NULL LIMIT ?;",
        conn,
        params=(limite,)
    )

    corpus = [
        f"{fila['origen']} | cie10={fila['cie10_code']} | {fila['texto']}"
        for _, fila in df.iterrows()
    ]

    vectorizer = TfidfVectorizer(lowercase=True, ngram_range=(1, 2))
    X = vectorizer.fit_transform(corpus)

    return vectorizer, X, corpus


def recuperar(query, vectorizer, X, corpus, k=6):
    """Recupera los fragmentos textuales más relevantes."""
    qv = vectorizer.transform([query])
    sims = cosine_similarity(qv, X)[0]
    idxs = sims.argsort()[::-1][:k]
    return [corpus[i] for i in idxs]


# =====================================================
# CARGAR RESULTADOS DESCRIPTIVOS
# =====================================================

def cargar_resultados_descriptivos():
    contexto = {}
    if not os.path.exists(DESCR_DIR):
        print("[WARN] No existe docs/consultas_descriptivas/.")
        return contexto

    for archivo in os.listdir(DESCR_DIR):
        if archivo.endswith(".csv"):
            nombre = archivo.replace(".csv", "")
            try:
                df = pd.read_csv(os.path.join(DESCR_DIR, archivo))
                contexto[nombre] = df
            except Exception as e:
                print(f"[WARN] No se pudo leer {archivo}: {e}")

    print(f"[OK] Se cargaron {len(contexto)} archivos descriptivos.")
    return contexto


def obtener_contexto_predictivo(ctx_dict, i):
    """Devuelve un DataFrame usado como contexto para una pregunta predictiva."""
    if not ctx_dict:
        return "No hay datos descriptivos disponibles."

    claves = sorted(ctx_dict.keys())
    idx = (i - 1) % len(claves)
    return ctx_dict[claves[idx]].head(15).to_string(index=False)


# =====================================================
# PROMPT PARA PREGUNTAS PREDICTIVAS
# =====================================================

def generar_prompt_predictivo(pregunta, contexto_sql, evidencias):
    return f"""
Eres un analista epidemiológico especializado en salud pública.
Debes generar una predicción basada EXCLUSIVAMENTE en:

1) Datos reales provenientes del análisis descriptivo (tablas CSV)
2) Evidencias textuales relevantes recuperadas vía RAG
3) Conexiones conocidas del grafo (solo si aparecen en las evidencias)

Tu objetivo es:
- identificar patrones,
- sugerir causas probables,
- detectar riesgos emergentes,
- y proyectar tendencias a 5-10 años.

────────────────────────────────────────────
PREGUNTA PREDICTIVA:
{pregunta}

────────────────────────────────────────────
DATOS DESCRIPTIVOS REALES (CSV):
{contexto_sql}

────────────────────────────────────────────
EVIDENCIAS TEXTUALES:
{chr(10).join("- " + e for e in evidencias)}

────────────────────────────────────────────

Responde de forma profunda, clara y en español académico.
Incluye:
- patrones observados,
- explicaciones causales,
- proyección a futuro,
- implicaciones clínicas o epidemiológicas,
- recomendaciones de vigilancia o acción temprana.
"""


# =====================================================
# EJECUCIÓN DE UNA PREGUNTA PREDICTIVA
# =====================================================

def ejecutar_predictiva(pregunta, contexto_sql, evidencias):
    prompt = generar_prompt_predictivo(pregunta, contexto_sql, evidencias)

    response = ollama.chat(
        model=OLLAMA_MODEL,
        messages=[
            {"role": "system", "content": "Eres un analista experto en salud pública."},
            {"role": "user", "content": prompt}
        ]
    )

    return response["message"]["content"].strip()


def guardar_resultado(idx, pregunta, respuesta, evidencias, contexto_sql):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    nombre = f"{idx:02d}__{pregunta[:60].replace(' ','_')}.csv"

    ruta = os.path.join(OUTPUT_DIR, nombre)

    pd.DataFrame({
        "pregunta": [pregunta],
        "respuesta": [respuesta],
        "contexto_sql": [contexto_sql],
        "fragmentos": ["\n".join(evidencias)]
    }).to_csv(ruta, index=False)

    print(f"[OK] Guardado: {ruta}")


# =====================================================
# PREGUNTAS PREDICTIVAS OFICIALES
# =====================================================

PREGUNTAS_PREDICTIVAS_NUEVAS = [
    "Basado en la evolución del consumo excesivo de alcohol en mujeres (2011–2016), ¿qué consecuencias de salud pública se pueden predecir para los próximos 5–10 años?",
    "Explica por qué algunas entidades tienen altas urgencias por F16 pero bajas defunciones. Propón dos hipótesis opuestas.",
    "Integra la co-ocurrencia de F12 y F15 (SQL) y la jerga textual asociada. ¿Representa esto un riesgo emergente de policonsumo?",
    "Una nueva jerga no mapeada aparece en zonas con historial F19. ¿Podría ser una nueva droga sintética? Propón posible CIE-10.",
    "Con base en las defunciones por F15 en grupos de edad jóvenes, redacta un mensaje de campaña preventiva NO estigmatizante.",
    "SQL vs grafo muestran discrepancia entre F11 y F16. ¿Podría indicar un patrón emergente local no capturado internacionalmente?",
    "México tiene fragmentación de datos clínicos. ¿Cuál sería el mayor obstáculo para escalar este sistema a nivel nacional?",
    "Si hoy aparecen múltiples menciones de 'cocaína rosa' o 'sobredosis por fiesta', ¿constituye esto una alerta temprana?",
    "Distintas zonas tienen puntos calientes para F14 y F11. ¿Qué intervención diferenciada sería óptima (prevención vs reducción de daños)?",
    "El sistema combina SQL + texto + grafo. ¿Cuál es el mayor riesgo ético (p.ej. re-identificación) y cómo mitigarlo?"
]


# =====================================================
# MAIN
# =====================================================

def main():
    print("\n=== Iniciando consultas predictivas ===\n")

    conn = sqlite3.connect(DB_PATH)

    # Cargar corpus textual
    vectorizer, X, corpus = cargar_corpus(conn)
    print(f"[OK] Corpus vectorizado ({len(corpus)} documentos)\n")

    # Cargar resultados descriptivos como contexto REAL
    ctx_desc = cargar_resultados_descriptivos()

    # Ejecutar cada predictiva
    for i, pregunta in enumerate(PREGUNTAS_PREDICTIVAS_NUEVAS, start=1):
        print(f"\n--- Pregunta predictiva {i}/{len(PREGUNTAS_PREDICTIVAS_NUEVAS)} ---")
        print(pregunta)

        evidencias = recuperar(pregunta, vectorizer, X, corpus)
        contexto_sql = obtener_contexto_predictivo(ctx_desc, i)

        respuesta = ejecutar_predictiva(pregunta, contexto_sql, evidencias)

        guardar_resultado(i, pregunta, respuesta, evidencias, contexto_sql)

    conn.close()
    print("\n=== PREDICTIVAS COMPLETADAS ===")


if __name__ == "__main__":
    main()
