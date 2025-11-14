#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
LLM ORQUESTADOR DE CONSULTAS HETEROGÉNEAS (Versión OLLAMA - Estable)
------------------------------------------------------------------
- Usa modelo local 'mistral' vía Ollama.
- Detecta y corrige errores SQL comunes.
- Incluye fallback automático para P2–P5.
"""

import os, re, json, time, pickle
from datetime import datetime
import pandas as pd
import networkx as nx
from sqlalchemy import create_engine
import ollama

# === Configuración ===
DB_URL = "sqlite:///salud_federada.db"
GRAPH_PATH = "docs/grafo_comorbilidad.gpickle"
OUTPUT_DIR = "docs/llm_resultados_ollama"
MODELO_OLLAMA = "mistral"

os.makedirs(OUTPUT_DIR, exist_ok=True)
engine = create_engine(DB_URL)

try:
    with open(GRAPH_PATH, "rb") as f:
        G = pickle.load(f)
    print(f"📘 Grafo cargado con {len(G.nodes())} nodos y {len(G.edges())} aristas.")
except Exception as e:
    print(f"⚠️  No se pudo cargar el grafo: {e}")
    G = None

PREGUNTAS = [
    "¿Cuál es la evolución temporal del número de defunciones por F10 (alcohol) entre 2011 y 2016, diferenciando por sexo?",
    "¿Qué entidades federativas presentan las mayores tasas de urgencias por F12 (cannabis)?",
    "¿Cuál es la proporción de defunciones por F14 (cocaína) respecto al total de muertes por sustancias en 2015?",
    "¿En qué grupos de edad se concentran las defunciones por F15 (estimulantes)?",
    "¿Qué diagnósticos presentan el mayor incremento relativo de casos entre 2011 y 2016?",
    "¿Qué códigos CIE-10 son los nodos más centrales del grafo de comorbilidad (grado y betweenness)?",
    "¿Qué sustancias se encuentran más conectadas con F10 (alcohol)?",
    "¿Existen comunidades o clústeres que agrupen sustancias similares según sus conexiones en el grafo?",
]

# === Prompt ===
def preguntar_al_llm(pregunta: str, modelo: str) -> dict:
    system_prompt = """
Eres un asistente que responde SOLO con JSON válido. 
Campos obligatorios: "tipo" y "codigo".

- Usa tipo="sql" para consultas a la tabla v_eventos(anio, sexo, entidad_norm, edad_quinquenal, cie10_code, valor, fuente).
- Usa tipo="grafo" para análisis estructurales (fn: centralidad, betweenness, aristas, comunidades).

Ejemplos válidos:
{"tipo": "sql", "codigo": "SELECT anio, sexo, SUM(valor) FROM v_eventos WHERE cie10_code LIKE 'F10%' GROUP BY anio, sexo"}
{"tipo": "grafo", "codigo": {"fn": "centralidad"}}
"""
    try:
        r = ollama.chat(
            model=modelo,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": pregunta},
            ],
            format="json"
        )
        js = r.get("message", {}).get("content", "")
        if '"type"' in js and '"tipo"' not in js:
            js = js.replace('"type"', '"tipo"')
        return json.loads(js)
    except Exception as e:
        return {"tipo": "error", "codigo": str(e)}

# === Grafo ===
def run_graph_op(op: dict) -> pd.DataFrame:
    fn = op.get("fn", "centralidad")
    if G is None:
        return pd.DataFrame({"error": ["Grafo no cargado."]})
    if fn == "centralidad":
        dc = nx.degree_centrality(G)
        return pd.DataFrame(dc.items(), columns=["nodo", "valor"]).sort_values("valor", ascending=False)
    if fn == "betweenness":
        bc = nx.betweenness_centrality(G)
        return pd.DataFrame(bc.items(), columns=["nodo", "valor"]).sort_values("valor", ascending=False)
    if fn == "aristas":
        return pd.DataFrame(list(G.edges()), columns=["source", "target"])
    if fn == "comunidades":
        comms = list(nx.community.greedy_modularity_communities(G))
        return pd.DataFrame({"comunidad": [list(c) for c in comms]})
    return pd.DataFrame({"error": [f"Operación no reconocida: {fn}"]})

# === Fallbacks ===
def fallback_sql(idx):
    if idx == 2:
        return """SELECT entidad_norm, SUM(valor) AS total
FROM v_eventos
WHERE fuente = 'urgencias' AND cie10_code LIKE 'F12%'
GROUP BY entidad_norm
ORDER BY total DESC
LIMIT 10"""
    if idx == 3:
        return """SELECT
  100.0 * SUM(CASE WHEN cie10_code LIKE 'F14%' THEN valor ELSE 0 END)
  / SUM(valor) AS porcentaje
FROM v_eventos
WHERE fuente='defunciones' AND anio=2015"""
    if idx == 5:
        return """WITH t1 AS (
  SELECT cie10_code, SUM(valor) AS total_2011
  FROM v_eventos WHERE anio=2011 GROUP BY cie10_code
),
t6 AS (
  SELECT cie10_code, SUM(valor) AS total_2016
  FROM v_eventos WHERE anio=2016 GROUP BY cie10_code
)
SELECT t1.cie10_code,
       (CAST(t6.total_2016 AS REAL) - t1.total_2011) / t1.total_2011 AS incremento
FROM t1 JOIN t6 ON t1.cie10_code = t6.cie10_code
WHERE t1.total_2011 > 0
ORDER BY incremento DESC
LIMIT 10"""
    return None

# === Ejecución ===
def ejecutar_respuesta_llm(resultado: dict, idx: int):
    tipo = resultado.get("tipo", "error")
    codigo = resultado.get("codigo", "")
    if tipo == "sql":
        codigo_limpio = str(codigo).strip().strip('[]')
        # --- Autocorrección de errores comunes ---
        codigo_limpio = codigo_limpio.replace("= 'F", "LIKE 'F")
        codigo_limpio = re.sub(r"HAVING\s+COUNT\(\*\)\s*=\s*1", "", codigo_limpio, flags=re.I)
        codigo_limpio = re.sub(r"cie10_code\s*(?=\s*,|\s+FROM)", "t_2011.cie10_code", codigo_limpio)

        try:
            return pd.read_sql(codigo_limpio, engine), codigo_limpio
        except Exception as e:
            fb = fallback_sql(idx)
            if fb:
                print(f"  ⚙️  Usando fallback seguro para P{idx}")
                try:
                    return pd.read_sql(fb, engine), fb
                except Exception as e2:
                    return pd.DataFrame({"error": [f"Error SQL fallback: {e2}"]}), fb
            return pd.DataFrame({"error": [f"Error SQL: {e}"]}), codigo_limpio
    elif tipo == "grafo":
        return run_graph_op(codigo), f"Grafo: {codigo}"
    else:
        return pd.DataFrame({"error": [f"Tipo no reconocido: {tipo}"]}), str(codigo)

def normalize_and_dedup(df: pd.DataFrame):
    if df is None or df.empty:
        return pd.DataFrame()
    df = df.rename(columns={c: c.strip().lower() for c in df.columns})
    for c in df.columns:
        if df[c].dtype == "object":
            df[c] = df[c].astype(str).str.strip()
    return df.drop_duplicates().reset_index(drop=True)

# === Loop principal ===
def main():
    print(f"=== SISTEMA LLM ORQUESTADOR (Ollama Estable) ===")
    summary = []
    for i, pregunta in enumerate(PREGUNTAS, start=1):
        print(f"\n--- Pregunta {i}/{len(PREGUNTAS)} ---\n{pregunta}")
        t0 = time.time()
        r = preguntar_al_llm(pregunta, MODELO_OLLAMA)
        print(f"  🤖 LLM generó: {r}")
        df, desc = ejecutar_respuesta_llm(r, i)
        df = normalize_and_dedup(df)
        out = os.path.join(OUTPUT_DIR, f"{i:02d}_{re.sub(r'[^A-Za-z0-9_]+','_',pregunta)[:60]}.csv")
        if "error" in df.columns:
            print(f"  ❌ {df['error'].iloc[0]}")
        else:
            df.to_csv(out, index=False)
            print(f"  ✅ Guardado en {out} ({df.shape[0]} filas)")
        summary.append({"idx": i, "pregunta": pregunta, "sql": desc, "rows": len(df)})
    with open(os.path.join(OUTPUT_DIR, "_summary.json"), "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)
    print("\n📦 ¡Proceso completado! Resumen guardado en docs/llm_resultados_ollama/_summary.json")

if __name__ == "__main__":
    main()
