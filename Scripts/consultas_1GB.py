#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
consultas_rag_predictivas_corpus_mixto.py
---------------------------------------------------------------
Responde las 10 preguntas PREDICTIVAS del proyecto usando:

- Texto de la vista v_unificado (SQLite, salud_federada.db)
- Texto de un corpus externo grande: CoWeSe1GB.txt
- Recuperación de contexto vía RAG (TF-IDF)
- LLM (Ollama, modelo mistral) para generar la respuesta final

NO usa:
- Resultados de consultas descriptivas (CSV)
- Scripts consultas_descriptivas.py ni consultas_rag_predictivas.py

Salida:
    docs/llm_resultados_predictivas_corpus_mixto/*.csv
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
CORPUS_TXT_PATH = "CoWeSe1GB.txt"   # El archivo de ~1GB
OUTPUT_DIR = "docs/llm_resultados_predictivas_corpus_mixto"
os.makedirs(OUTPUT_DIR, exist_ok=True)

OLLAMA_MODEL = "mistral"


# =====================================================
# 1. CARGAR CORPUS MIXTO (v_unificado + CoWeSe1GB.txt)
# =====================================================

def cargar_corpus_mixto(conn,
                        limite_sql=50000,
                        max_lineas_txt=200000):
    """
    Construye un corpus RAG combinando:

    1) Una muestra aleatoria de registros con texto desde v_unificado.
    2) Las primeras 'max_lineas_txt' líneas NO vacías del archivo CoWeSe1GB.txt.

    Ajusta 'limite_sql' y 'max_lineas_txt' según RAM / tiempo.
    Con 64GB deberías aguantar bastante bien estos valores.
    """

    corpus = []

    # ---------- 1) TEXTO DESDE v_unificado ----------
    print(f"[INFO] Leyendo texto desde v_unificado (limite_sql={limite_sql})...")
    df_sql = pd.read_sql_query(
        """
        SELECT origen, cie10_code, texto
        FROM v_unificado
        WHERE texto IS NOT NULL
        ORDER BY RANDOM()
        LIMIT ?;
        """,
        conn,
        params=(limite_sql,)
    )

    for _, fila in df_sql.iterrows():
        origen = fila["origen"]
        cie = fila["cie10_code"]
        txt = str(fila["texto"])
        corpus.append(f"{origen} | cie10={cie} | {txt}")

    print(f"[OK] v_unificado aportó {len(df_sql)} fragmentos.")

    # ---------- 2) TEXTO DESDE CoWeSe1GB.txt ----------
    if os.path.exists(CORPUS_TXT_PATH):
        print(f"[INFO] Leyendo texto desde {CORPUS_TXT_PATH} (hasta {max_lineas_txt} líneas no vacías)...")
        with open(CORPUS_TXT_PATH, "r", encoding="utf-8", errors="ignore") as f:
            count = 0
            for line in f:
                linea = line.strip()
                if not linea:
                    continue
                corpus.append(f"cowese_1gb | cie10=? | {linea}")
                count += 1
                if count >= max_lineas_txt:
                    break
        print(f"[OK] CoWeSe1GB.txt aportó {count} fragmentos.")
    else:
        print(f"[WARN] No se encontró {CORPUS_TXT_PATH}. Solo se usará v_unificado.")

    # ---------- 3) VECTORIZAR ----------
    print(f"[INFO] Corpus total para RAG: {len(corpus)} fragmentos.")
    print("[INFO] Vectorizando con TF-IDF (esto puede tardar un poco)...")

    vectorizer = TfidfVectorizer(lowercase=True, ngram_range=(1, 2))
    X = vectorizer.fit_transform(corpus)

    print("[OK] Vectorización completada.")
    return vectorizer, X, corpus


# =====================================================
# 2. RECUPERAR EVIDENCIAS VIA TF-IDF
# =====================================================

def recuperar(query, vectorizer, X, corpus, k=10):
    """
    Recupera los K fragmentos más relevantes del corpus mixto
    (v_unificado + CoWeSe1GB.txt) para una pregunta dada.
    """
    qv = vectorizer.transform([query])
    sims = cosine_similarity(qv, X)[0]
    idxs = sims.argsort()[::-1][:k]
    return [corpus[i] for i in idxs]


# =====================================================
# 3. PROMPT PARA PREGUNTAS PREDICTIVAS
# =====================================================

def generar_prompt_predictivo(pregunta, evidencias):
    """
    Construye el prompt para el LLM usando SOLO evidencias textuales
    (no usamos aquí los CSV descriptivos).
    """
    return f"""
Eres un analista epidemiológico experto en salud pública.

Debes responder la siguiente pregunta PREDICTIVA basándote
EXCLUSIVAMENTE en evidencias textuales recuperadas mediante RAG
de un corpus mixto que incluye:

- Frases clínicas y descriptivas ligadas a códigos CIE-10 (v_unificado).
- Frases del corpus CoWeSe (archivo CoWeSe1GB.txt).

────────────────────────────────────────────
PREGUNTA PREDICTIVA:
{pregunta}

────────────────────────────────────────────
EVIDENCIAS TEXTUALES RELEVANTES:
{chr(10).join("- " + e for e in evidencias)}

────────────────────────────────────────────

Responde en español académico, incluyendo:
- patrones observados en las evidencias,
- posibles explicaciones causales,
- proyección a 5–10 años,
- riesgos emergentes,
- recomendaciones de vigilancia o intervención.
"""


# =====================================================
# 4. LLAMADA AL LLM
# =====================================================

def ejecutar_predictiva(pregunta, evidencias):
    prompt = generar_prompt_predictivo(pregunta, evidencias)

    response = ollama.chat(
        model=OLLAMA_MODEL,
        messages=[
            {"role": "system", "content": "Eres un analista experto en salud pública."},
            {"role": "user", "content": prompt}
        ]
    )

    return response["message"]["content"].strip()


def guardar_resultado(idx, pregunta, respuesta, evidencias):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    nombre = f"{idx:02d}__{pregunta[:60].replace(' ','_')}.csv"
    ruta = os.path.join(OUTPUT_DIR, nombre)

    pd.DataFrame({
        "pregunta": [pregunta],
        "respuesta": [respuesta],
        "fragmentos": ["\n".join(evidencias)]
    }).to_csv(ruta, index=False)

    print(f"[OK] Guardado: {ruta}")


# =====================================================
# 5. PREGUNTAS PREDICTIVAS
# =====================================================

PREGUNTAS_PREDICTIVAS = [
    "Basado en la evolución del consumo excesivo de alcohol en mujeres, ¿qué consecuencias de salud pública se pueden predecir para los próximos 5–10 años?",
    "Explica por qué algunas entidades podrían tener altas urgencias por F16 (alucinógenos) pero bajas defunciones. Propón dos hipótesis opuestas.",
    "Integra la co-ocurrencia de patrones relacionados con cannabis (F12) y estimulantes (F15) que aparezcan en el corpus. ¿Representa esto un riesgo emergente de policonsumo?",
    "Si en regiones con historial de F19 (solventes) aparece nueva jerga no mapeada, ¿podría indicar una nueva droga sintética? Propón un posible código CIE-10 asociado.",
    "Con base en patrones textuales asociados a F15 en población joven, redacta un mensaje de campaña preventiva NO estigmatizante.",
    "Si el corpus sugiere discrepancias entre el uso de opioides (F11) y alucinógenos (F16), ¿podría esto anticipar un patrón local de policonsumo no descrito en la literatura internacional?",
    "Considerando las referencias a problemas estructurales en el sistema de salud mexicano, ¿cuál sería el obstáculo más importante para escalar un sistema federado nacional de datos clínicos?",
    "Si el corpus contiene múltiples menciones recientes de “sobredosis por fiesta” o “cocaína rosa”, ¿constituye esto una alerta temprana? ¿Qué acciones debería tomar un analista de salud pública?",
    "A partir de menciones diferenciadas de cocaína (F14) y opioides (F11) en distintas zonas, ¿qué tipo de intervención diferenciada (prevención vs reducción de daños) sería más adecuada?",
    "El sistema integra datos clínicos, texto libre y relaciones entre diagnósticos. ¿Cuál es el riesgo ético más relevante (p.ej. re-identificación, estigmatización) y cómo mitigarlo?"
]


# =====================================================
# 6. MAIN
# =====================================================

def main():
    print("\n=== Iniciando predictivas con corpus mixto (v_unificado + CoWeSe1GB) ===\n")

    # Conexión a la base (solo para leer v_unificado)
    conn = sqlite3.connect(DB_PATH)

    # Construir corpus mixto para RAG
    vectorizer, X, corpus = cargar_corpus_mixto(
        conn,
        limite_sql=50000,      # puedes subir/bajar estos valores
        max_lineas_txt=200000  # según qué tan pesado quieras que sea
    )

    # Ejecutar cada pregunta predictiva
    for i, pregunta in enumerate(PREGUNTAS_PREDICTIVAS, start=1):
        print(f"\n--- Pregunta predictiva {i}/{len(PREGUNTAS_PREDICTIVAS)} ---")
        print(pregunta)

        evidencias = recuperar(pregunta, vectorizer, X, corpus, k=10)
        respuesta = ejecutar_predictiva(pregunta, evidencias)
        guardar_resultado(i, pregunta, respuesta, evidencias)

    conn.close()
    print("\n=== PREDICTIVAS (CORPUS MIXTO) COMPLETADAS ===\n")


if __name__ == "__main__":
    main()
