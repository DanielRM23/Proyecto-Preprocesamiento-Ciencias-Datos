#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
consultas_codigo_llm.py
---------------------------------------------------------------
Generación de código heterogéneo (SQL / GRAFO / VECTORIAL) a partir
de las 20 preguntas de negocio usando un LLM (Ollama).

Objetivo (parte "Generación de consultas heterogéneas con LLM"):

a) Traducir las 20 preguntas en lenguaje natural a:
   - código SQL (sobre salud_federada.db)
   - código para grafo (NetworkX)
   - descripción vectorial (búsqueda en texto)
b) El LLM reconoce qué tipo de código corresponde a cada pregunta.
c) El código SQL se ejecuta realmente contra la base SQLite.
d) Se reúnen las respuestas; para SQL se guardan resultados y errores.
e) Se entrega el resultado al usuario en forma de CSVs en docs/consultas_llm_codigo.

Requisitos:
- Base de datos: salud_federada.db
- Ollama instalado y corriendo
- Modelo: mistral (configurable)
"""

import os
import re
import json
import sqlite3
from datetime import datetime

import pandas as pd
import ollama

# =====================================================
# CONFIGURACIÓN
# =====================================================

DB_PATH = "salud_federada.db"
OUTPUT_DIR = "docs/consultas_llm_codigo"
os.makedirs(OUTPUT_DIR, exist_ok=True)

OLLAMA_MODEL = "mistral"
PREGUNTAS_CSV = os.path.join(OUTPUT_DIR, "preguntas_20.csv")  # opcional


# =====================================================
# PROMPT DEL LLM PARA GENERAR CÓDIGO
# =====================================================

PROMPT_CODIGO = """
Eres un sistema experto en convertir preguntas en lenguaje natural a CÓDIGO ejecutable sobre una base de datos YA DEFINIDA.

MUY IMPORTANTE:
Solo puedes usar las SIGUIENTES TABLAS y VISTAS (SQLite):

Tablas:
- fact_defunciones(anio, entidad_norm, edad_quinquenal, sexo, cie10_code, valor)
- fact_urgencias(anio, entidad_norm, edad_quinquenal, sexo, cie10_code, valor)
- cie10_nodes(code, descripcion)
- cie10_edges(source, target, rel_type)
- texto_frases(cie10_code, sentence_norm, n_ocurrencias, phrase_hash)
- texto_frases_x_docs(phrase_hash, doc_id, sent_id)

Vistas:
- v_eventos(anio, entidad_norm, edad_quinquenal, sexo, cie10_code, valor, fuente)
- v_eventos_por_codigo(cie10_code, fuente, total)
- v_relaciones_epidemiologicas(cie10_a, cie10_b, tipo)
- v_texto_por_codigo(cie10_code, sentence_norm, n_ocurrencias)
- v_unificado(origen, id_origen, cie10_code, anio, entidad_norm, edad_quinquenal, sexo, valor, fuente, texto, campo)

REGLAS ESTRICTAS:
1. Solo genera código SQL usando estas tablas o vistas.
2. NO inventes columnas como “year”, “ICD_10”, “deaths”.
3. NO inventes tablas nuevas.
4. Para datos SQL usa SIEMPRE estas columnas:
   - anio
   - entidad_norm
   - edad_quinquenal
   - sexo
   - cie10_code
   - valor

FORMATO DE RESPUESTA:
Devuelve SIEMPRE tres bloques (aunque alguno quede vacío), con este formato EXACTO:

<SQL>
-- aquí el código SQL, o vacío si no aplica
</SQL>

<GRAFO>
# aquí el código de Python/NetworkX, o comentario si no aplica
</GRAFO>

<VEC>
# aquí instrucciones o pseudo-código para búsqueda vectorial, o comentario si no aplica
</VEC>

NO EXPLIQUES NADA. NO COMENTES FUERA DE LOS BLOQUES. SOLO CÓDIGO DENTRO DE LOS TAGS.

Pregunta:
{PREGUNTA}
"""


# =====================================================
# PREGUNTAS DE NEGOCIO (FALLBACK)
# Si existe un CSV con las 20 preguntas, se lee desde ahí.
# Si no, se usa esta lista por defecto.
# =====================================================

PREGUNTAS_20_FALLBACK = [
    "¿Cuál es la evolución temporal de defunciones por F10 entre 2011 y 2016?",
    "¿Qué entidades federativas presentan las mayores urgencias por F12 (cannabis) en 2015?",
    "¿Cuál es la proporción de defunciones por F14 (cocaína) respecto al total de muertes por sustancias en 2015?",
    "¿En qué grupos de edad se concentran las defunciones por F15 (estimulantes)?",
    "¿Qué diagnósticos presentan el mayor incremento relativo entre 2011 y 2016?",
    "¿Qué códigos CIE-10 son los nodos más centrales del grafo de comorbilidad?",
    "¿Qué sustancias se encuentran más conectadas con F10 (alcohol) en el grafo?",
    "¿Existen comunidades o clústeres que agrupen sustancias similares en el grafo?",
    "¿Qué tan fuerte es la conexión entre F12 (cannabis) y F20–F29 (trastornos psicóticos)?",
    "¿Qué pares de diagnósticos tienen la mayor coocurrencia según el grafo de comorbilidad?",
    "¿Qué palabras son más frecuentes en frases asociadas con F10 (alcohol)?",
    "¿Qué frases del corpus mencionan simultáneamente términos asociados con dependencia y opioides?",
    "¿Qué códigos CIE-10 aparecen más referenciados en el corpus textual?",
    "¿Qué términos aparecen más próximos (semánticamente) a crack o piedra?",
    "¿Qué porcentaje de frases menciona más de una sustancia psicoactiva?",
    "¿Los diagnósticos más centrales en el grafo de comorbilidad son también los que tienen mayor número de defunciones?",
    "¿Qué pares de diagnósticos con alta coocurrencia en el grafo se mencionan juntos en el corpus textual?",
    "¿Qué entidades con mayor número de urgencias por F16 (alucinógenos) aparecen en frases con lenguaje de alarma o gravedad?",
    "¿Qué combinaciones de sustancias según el grafo se mencionan frecuentemente juntas en el corpus textual?",
    "Según los patrones del grafo y el corpus textual, ¿qué nuevas combinaciones de sustancias podrían representar riesgo emergente de policonsumo?"
]


# =====================================================
# FUNCIONES AUXILIARES
# =====================================================

def cargar_preguntas():
    """
    Intenta cargar las 20 preguntas desde un CSV.
    Si no existe, usa la lista de fallback.
    Formato esperado del CSV:
        columna 'pregunta'
    """
    if os.path.exists(PREGUNTAS_CSV):
        try:
            df = pd.read_csv(PREGUNTAS_CSV)
            if "pregunta" in df.columns:
                preguntas = df["pregunta"].dropna().tolist()
                if preguntas:
                    print(f"[INFO] Preguntas cargadas desde {PREGUNTAS_CSV} ({len(preguntas)} filas).")
                    return preguntas
        except Exception as e:
            print(f"[WARN] No se pudo leer {PREGUNTAS_CSV}: {e}")

    print("[INFO] Usando lista interna de 20 preguntas.")
    return PREGUNTAS_20_FALLBACK


def pedir_codigo_llm(pregunta: str) -> str:
    """
    Llama a Ollama con el PROMPT_CODIGO para generar bloques:
    <SQL>...</SQL>, <GRAFO>...</GRAFO>, <VEC>...</VEC>
    """
    prompt = PROMPT_CODIGO.replace("{PREGUNTA}", pregunta)

    resp = ollama.chat(
        model=OLLAMA_MODEL,
        messages=[
            {
                "role": "system",
                "content": "Eres un generador de código estricto. Cumple EXACTAMENTE las reglas del prompt."
            },
            {"role": "user", "content": prompt}
        ]
    )
    contenido = resp["message"]["content"]
    return contenido


def extraer_bloque(texto: str, tag: str) -> str:
    """
    Extrae el contenido dentro de <TAG>...</TAG> usando regex.
    Si no encuentra el bloque, regresa cadena vacía.
    """
    patron = rf"<{tag}>(.*?)</{tag}>"
    m = re.search(patron, texto, flags=re.DOTALL | re.IGNORECASE)
    if not m:
        return ""
    bloque = m.group(1).strip()
    return bloque


def ejecutar_sql(conn, sql_code: str):
    """
    Ejecuta código SQL (si no está vacío) contra la BD.
    Devuelve (df, error_str).
    - df: DataFrame con resultados o None si fallo.
    - error_str: None si OK, o mensaje de error.
    """
    sql_code = (sql_code or "").strip()
    if not sql_code:
        return None, "SQL vacío (no se generó código SQL para esta pregunta)."

    try:
        df = pd.read_sql_query(sql_code, conn)
        return df, None
    except Exception as e:
        return None, str(e)


def guardar_resultados_pregunta(idx: int,
                                pregunta: str,
                                codigo_llm: str,
                                sql_code: str,
                                grafo_code: str,
                                vec_code: str,
                                df_sql: pd.DataFrame | None,
                                sql_error: str | None):
    """
    Guarda, para una pregunta:
    - Archivo resumen: qXX_resumen.csv
    - Código crudo del LLM: qXX_codigo_llm.txt
    - Resultados SQL (si hubo): qXX_sql_resultados.csv
    """
    base = f"q{idx:02d}"

    # 1) Código crudo del LLM
    ruta_codigo = os.path.join(OUTPUT_DIR, f"{base}_codigo_llm.txt")
    with open(ruta_codigo, "w", encoding="utf-8") as f:
        f.write(codigo_llm)
    print(f"[OK] Código LLM guardado en {ruta_codigo}")

    # 2) Resumen (una fila)
    resumen = {
        "pregunta": pregunta,
        "sql_code": sql_code,
        "grafo_code": grafo_code,
        "vectorial_code": vec_code,
        "sql_error": sql_error if sql_error else "",
        "sql_tiene_resultados": bool(df_sql is not None and not df_sql.empty),
        "timestamp": datetime.now().isoformat(timespec="seconds")
    }
    df_resumen = pd.DataFrame([resumen])
    ruta_resumen = os.path.join(OUTPUT_DIR, f"{base}_resumen.csv")
    df_resumen.to_csv(ruta_resumen, index=False)
    print(f"[OK] Resumen guardado en {ruta_resumen}")

    # 3) Resultados SQL
    if df_sql is not None and not df_sql.empty:
        ruta_sql = os.path.join(OUTPUT_DIR, f"{base}_sql_resultados.csv")
        df_sql.to_csv(ruta_sql, index=False)
        print(f"[OK] Resultados SQL guardados en {ruta_sql}")
    else:
        print("[INFO] Sin resultados SQL para guardar (o DataFrame vacío).")


# =====================================================
# MAIN
# =====================================================

def main():
    preguntas = cargar_preguntas()

    conn = sqlite3.connect(DB_PATH)
    try:
        for i, pregunta in enumerate(preguntas, start=1):
            print(f"\n=== Pregunta {i}/{len(preguntas)} ===")
            print("→", pregunta)

            # 1) Pedir código al LLM
            codigo_llm = pedir_codigo_llm(pregunta)
            print("[DEBUG] Respuesta LLM recibida.")

            # 2) Extraer cada bloque
            sql_code = extraer_bloque(codigo_llm, "SQL")
            grafo_code = extraer_bloque(codigo_llm, "GRAFO")
            vec_code = extraer_bloque(codigo_llm, "VEC")

            # 3) Ejecutar SQL
            df_sql, sql_error = ejecutar_sql(conn, sql_code)
            if sql_error:
                print(f"[WARN] Error al ejecutar SQL para la pregunta {i}: {sql_error}")
            else:
                print(f"[OK] SQL ejecutado correctamente para la pregunta {i}. "
                      f"Filas: {len(df_sql)}")

            # 4) Guardar todo
            guardar_resultados_pregunta(
                idx=i,
                pregunta=pregunta,
                codigo_llm=codigo_llm,
                sql_code=sql_code,
                grafo_code=grafo_code,
                vec_code=vec_code,
                df_sql=df_sql,
                sql_error=sql_error
            )

    finally:
        conn.close()
        print("\n[FIN] Procesamiento de las 20 preguntas completado.")


if __name__ == "__main__":
    main()
