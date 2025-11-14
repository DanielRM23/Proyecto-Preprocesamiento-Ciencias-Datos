#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
consultas_descriptivas.py
---------------------------------------------------------------
Resuelve las 10 preguntas DESCRIPTIVAS del proyecto usando SQL + pandas,
y genera gráficas con un estilo profesional, márgenes ampliados,
tipografía grande y sin texto cortado.

Salida:
- CSV por pregunta en: docs/consultas_descriptivas/
- Gráficas en: docs/figuras_descriptivas/
"""

import os
import sqlite3
import re
from collections import Counter

import pandas as pd
import matplotlib.pyplot as plt

DB_PATH = "salud_federada.db"
OUT_DIR = "docs/consultas_descriptivas"
FIG_DIR = "docs/figuras_descriptivas"

os.makedirs(OUT_DIR, exist_ok=True)
os.makedirs(FIG_DIR, exist_ok=True)


# =====================================================
# CONFIGURACIÓN GLOBAL DE GRÁFICAS  ⭐⭐⭐⭐⭐
# =====================================================

plt.style.use("ggplot")  # Estilo profesional

plt.rcParams.update({
    "figure.figsize": (12, 7),
    "font.size": 12,
    "axes.labelsize": 14,
    "axes.titlesize": 16,
    "xtick.labelsize": 12,
    "ytick.labelsize": 12,
    "legend.fontsize": 12,
})

def ajustar_margenes():
    """Evita textos cortados en TODAS las gráficas."""
    plt.tight_layout()
    plt.subplots_adjust(left=0.30, right=0.95, top=0.90, bottom=0.20)


# =====================================================
# AUXILIARES GENERALES
# =====================================================

def conectar():
    return sqlite3.connect(DB_PATH)


def ejecutar_sql(conn, sql, params=None):
    try:
        df = pd.read_sql_query(sql, conn, params=params or [])
        return df
    except Exception as e:
        print(f"[ERROR] Falló la consulta SQL:\n{sql}\n{e}\n")
        return pd.DataFrame()


def guardar_csv(df, nombre_base):
    if df is None or df.empty:
        print(f"[WARN] Nada que guardar para {nombre_base}.")
        return
    ruta = os.path.join(OUT_DIR, f"{nombre_base}.csv")
    df.to_csv(ruta, index=False)
    print(f"[OK] CSV guardado: {ruta}")


def configurar_plt():
    plt.clf()


# =====================================================
# STOPWORDS (MEJORADAS)
# =====================================================

stopwords_expandidas_final = [
    'ajena','ajenas','ajeno','ajenos','algún','alguna','algunas','alguno','algunos','aquel',
    'aquella','aquellas','aquello','aquellos','cuanta','cuantas','cuanto','cuantos','demasiada',
    'demasiadas','demasiado','demasiados','ella','ellas','ello','ellos','misma','mismas','mismo',
    'mismos','muchísima','muchísimas','muchísimo','muchísimos','ningún','ninguna','ningunas',
    'ninguno','ningunos','nuestra','nuestras','nuestro','nuestros','otra','otras','otro','otros',
    'poca','pocas','poco','pocos','suya','suyas','suyo','suyos','tanta','tantas','tanto','tantos',
    'toda','todas','todo','todos','tuya','tuyas','tuyo','tuyos','un','una','unas','uno','unos',
    'vuestra','vuestras','vuestro','vuestros','o','mas','son','tambien', 'durante'
]

stopwords_resto_corregida_y_expandida = [
    'a','acá','ahí','al','algo','allí','allá','ambos','ante','antes','aquel','aquí','arriba',
    'así','atrás','aun','aunque','bajo','bastante','bien','cabe','cada','casi','cierto','cierta',
    'ciertos','ciertas','como','con','conmigo','conseguimos','conseguir','consigo','consigue',
    'consiguen','consigues','contigo','contra','cual','cuales','cualquier','cualquiera',
    'cualesquiera','cuan','cuando','de','dejar','del','demás','dentro','desde','donde','dos',
    'el','él','empleáis','emplean','emplear','empleas','empleo','en','encima','entonces','entre',
    'era','eras','eramos','eran','eres','es','esa','ese','eso','esas','esos','esta','estas',
    'estaba','estado','estáis','estamos','están','estar','este','esto','estos','estoy','etc',
    'fin','fue','fueron','fui','fuimos','gueno','ha','hace','haces','hacéis','hacemos','hacen',
    'hacer','hacia','hago','hasta','incluso','intenta','intentamos','intentan','intentar',
    'intento','ir','jamás','junto','juntos','la','lo','las','los','largo','más','me','menos',
    'mi','mis','mía','mías','mío','míos','mientras','modo','mucha','muchas','mucho','muchos',
    'muy','nada','ni','no','nos','nosotras','nosotros','nunca','para','parecer','pero','podéis',
    'podemos','poder','podría','podríamos','podrían','por','por qué','porque','primero','puede',
    'pueden','puedo','pues','que','qué','querer','quién','quienes','quienesquiera','quienquiera',
    'quizá','quizás','sabe','sabes','saben','sabéis','sabemos','saber','se','según','ser','si',
    'sí','siempre','siendo','sin','sino','so','sobre','sois','solamente','solo','somos','soy',
    'sr','sra','sres','sta','su','sus','tal','tales','también','tampoco','tan','te','tenéis',
    'tenemos','tener','tengo','ti','tiempo','tiene','tienen','tomar','trabaja','trabajo',
    'trabajáis','trabajamos','trabajan','trabajar','trabajas','tras','tú','tu','tus','último',
    'usa','usas','usáis','usamos','usan','usar','uso','usted','ustedes','va','van','vais','vamos',
    'varia','vario','varias','varios','vaya','verdadera','vosotras','vosotros','voy','y','ya','yo'
]

STOPWORDS_ES = set(stopwords_expandidas_final + stopwords_resto_corregida_y_expandida)

def tokenizar(texto):
    if not isinstance(texto, str):
        return []
    palabras = re.split(r"\W+", texto.lower())
    return [p for p in palabras if p and p not in STOPWORDS_ES]


# =====================================================
# PREGUNTAS (MODIFICADAS CON GRÁFICAS MEJORADAS)
# =====================================================

def pregunta_1(conn):
    print("\n[1] F10 por sexo...")
    sql = """
        SELECT anio, sexo, SUM(valor) AS total
        FROM fact_defunciones
        WHERE cie10_code LIKE 'F10%'
        AND anio BETWEEN 2011 AND 2016
        GROUP BY anio, sexo
        ORDER BY anio, sexo;
    """
    df = ejecutar_sql(conn, sql)
    guardar_csv(df, "pregunta01_evolucion_F10_por_sexo")
    if df.empty:
        return

    configurar_plt()
    for sexo, sub in df.groupby("sexo"):
        plt.plot(sub["anio"], sub["total"], marker="o", label=sexo)

    plt.title("Evolución de defunciones por F10 (2011–2016) por sexo")
    plt.xlabel("Año")
    plt.ylabel("Defunciones")
    plt.legend()
    ajustar_margenes()

    plt.savefig(os.path.join(FIG_DIR, "pregunta01.png"))
    print("[OK] pregunta01.png guardada")


def pregunta_2(conn):
    print("\n[2] F12 urgencias 2015...")
    sql = """
        SELECT entidad_norm, SUM(valor) AS total
        FROM fact_urgencias
        WHERE cie10_code LIKE 'F12%' AND anio = 2015
        GROUP BY entidad_norm
        ORDER BY total DESC;
    """
    df = ejecutar_sql(conn, sql)
    guardar_csv(df, "pregunta02_urgencias_F12_2015")
    if df.empty:
        return

    top = df.head(10)
    configurar_plt()
    plt.barh(top["entidad_norm"], top["total"])
    plt.gca().invert_yaxis()
    plt.title("Top entidades con urgencias por F12 (2015)")
    plt.xlabel("Urgencias")
    ajustar_margenes()

    plt.savefig(os.path.join(FIG_DIR, "pregunta02.png"))
    print("[OK] pregunta02.png guardada")


def pregunta_3(conn):
    print("\n[3] Proporción F14 en 2015...")
    sql1 = "SELECT SUM(valor) AS total FROM fact_defunciones WHERE anio=2015 AND cie10_code LIKE 'F14%';"
    sql2 = "SELECT SUM(valor) AS total FROM fact_defunciones WHERE anio=2015 AND SUBSTR(cie10_code,1,3) BETWEEN 'F10' AND 'F19';"

    f14 = ejecutar_sql(conn, sql1)
    total = ejecutar_sql(conn, sql2)

    df = pd.DataFrame([{
        "F14": f14.iloc[0, 0] if not f14.empty else 0,
        "Total": total.iloc[0, 0] if not total.empty else 0
    }])
    guardar_csv(df, "pregunta03_proporcion_F14")

    configurar_plt()
    plt.pie(df.iloc[0], labels=["F14", "Otras sustancias"], autopct='%1.1f%%')
    plt.title("Proporción F14 vs F10–F19 (2015)")
    ajustar_margenes()

    plt.savefig(os.path.join(FIG_DIR, "pregunta03.png"))
    print("[OK] pregunta03.png guardada")


def pregunta_4(conn):
    print("\n[4] F15 por edad...")
    sql = """
        SELECT edad_quinquenal, SUM(valor) AS total
        FROM fact_defunciones
        WHERE cie10_code LIKE 'F15%'
        GROUP BY edad_quinquenal
        ORDER BY total DESC;
    """
    df = ejecutar_sql(conn, sql)
    guardar_csv(df, "pregunta04_F15_por_edad")

    if df.empty:
        return

    # Filtrar grupos irrelevantes
    grupos_excluir = {
        "Menores de 1 año",
        "85 y mas años",
        "80 a 84 años",
        "10 a 14 años"
    }

    # Crear dataframe filtrado
    df_filtrado = df[~df["edad_quinquenal"].isin(grupos_excluir)]

    configurar_plt()

    plt.barh(df_filtrado["edad_quinquenal"], df_filtrado["total"])
    plt.gca().invert_yaxis()
    plt.xlabel("Defunciones por F15")
    plt.title("Defunciones F15 por grupo de edad")
    ajustar_margenes()

    fig_path = os.path.join(FIG_DIR, "pregunta04.png")
    plt.savefig(fig_path)
    print("[OK] pregunta04.png guardada:", fig_path)


def pregunta_5(conn):
    print("\n[5] Incremento 2011–2016...")
    sql = """
        SELECT anio, cie10_code, SUM(valor) AS total
        FROM fact_defunciones
        WHERE anio BETWEEN 2011 AND 2016
        GROUP BY anio, cie10_code;
    """
    df = ejecutar_sql(conn, sql)
    if df.empty:
        return

    tabla = df.pivot_table(index="cie10_code", columns="anio", values="total", fill_value=0)

    if 2011 not in tabla.columns or 2016 not in tabla.columns:
        return

    tabla["incremento"] = tabla[2016] - tabla[2011]
    resultado = tabla.sort_values("incremento", ascending=False).head(10)
    guardar_csv(resultado.reset_index(), "pregunta05_incremento_2011_2016")

    configurar_plt()
    plt.barh(resultado.index, resultado["incremento"])
    plt.gca().invert_yaxis()
    plt.title("Top 10 incrementos en defunciones (2011–2016)")
    plt.xlabel("Incremento")
    ajustar_margenes()

    plt.savefig(os.path.join(FIG_DIR, "pregunta05.png"))
    print("[OK] pregunta05.png guardada")


def pregunta_6(conn):
    print("\n[6] Palabras frecuentes F10...")
    sql = """
        SELECT sentence_norm, n_ocurrencias
        FROM texto_frases
        WHERE cie10_code='F10';
    """
    df = ejecutar_sql(conn, sql)
    guardar_csv(df, "pregunta06_frases_F10")

    if df.empty:
        return

    contador = Counter()
    for _, fila in df.iterrows():
        palabras = tokenizar(fila["sentence_norm"])
        peso = fila.get("n_ocurrencias", 1) or 1
        for p in palabras:
            contador[p] += peso

    top = pd.DataFrame(contador.most_common(15), columns=["palabra", "frecuencia"])
    guardar_csv(top, "pregunta06_top_palabras_F10")

    configurar_plt()
    plt.barh(top["palabra"], top["frecuencia"])
    plt.gca().invert_yaxis()
    plt.title("Top palabras asociadas a F10")
    plt.xlabel("Frecuencia ponderada")
    ajustar_margenes()

    plt.savefig(os.path.join(FIG_DIR, "pregunta06.png"))
    print("[OK] pregunta06.png guardada")


def pregunta_7(conn):
    print("\n[7] Frases que mencionan dependencia y opioides (F11)...")

    sql = """
        SELECT cie10_code, sentence_norm
        FROM texto_frases
        WHERE cie10_code LIKE 'F11%'
          AND sentence_norm LIKE '%dependenc%';
    """
    df = ejecutar_sql(conn, sql)
    guardar_csv(df, "pregunta07_frases_F11")

    if df.empty:
        print("[WARN] No se encontraron frases relevantes para F11.")
        return

    print(f"[INFO] Frases encontradas: {len(df)}")

    # --- Análisis básico de frecuencia de palabras ---
    contador = Counter()
    for frase in df["sentence_norm"]:
        palabras = tokenizar(frase)
        for p in palabras:
            contador[p] += 1

    top_palabras = contador.most_common(20)
    df_top = pd.DataFrame(top_palabras, columns=["palabra", "frecuencia"])
    guardar_csv(df_top, "pregunta07_top_palabras_F11")

    # --- Crear archivo Markdown con el análisis ---
    md_path = os.path.join(OUT_DIR, "pregunta07_reporte.md")
    with open(md_path, "w", encoding="utf-8") as f:
        f.write("# Pregunta 7 – Análisis de frases sobre dependencia y opioides (F11)\n\n")
        f.write(f"Total de frases encontradas: **{len(df)}**\n\n")

        f.write("## Ejemplos de frases encontradas\n")
        for frase in df["sentence_norm"].head(10):
            f.write(f"- {frase}\n")

        f.write("\n## Palabras más frecuentes (sin stopwords)\n")
        for palabra, freq in top_palabras[:10]:
            f.write(f"- **{palabra}**: {freq}\n")

    print(f"[OK] Archivo de análisis creado: {md_path}")



def pregunta_8(conn):
    print("\n[8] Códigos más mencionados...")
    sql = """
        SELECT cie10_code, SUM(COALESCE(n_ocurrencias,1)) AS menciones
        FROM texto_frases
        GROUP BY cie10_code
        ORDER BY menciones DESC;
    """
    df = ejecutar_sql(conn, sql)
    guardar_csv(df, "pregunta08_codigos_texto")

    if df.empty:
        return

    top = df.head(10)
    configurar_plt()
    plt.barh(top["cie10_code"], top["menciones"])
    plt.gca().invert_yaxis()
    plt.xlabel("Menciones")
    plt.title("Códigos más mencionados en texto")
    ajustar_margenes()

    plt.savefig(os.path.join(FIG_DIR, "pregunta08.png"))
    print("[OK] pregunta08.png guardada")


def pregunta_9(conn):
    print("\n[9] % frases multi-sustancia...")
    sql = """
        WITH frases AS (
            SELECT m.doc_id, m.sent_id,
                   COUNT(DISTINCT f.cie10_code) AS n_codigos
            FROM texto_frases_x_docs m
            JOIN texto_frases f ON f.phrase_hash = m.phrase_hash
            GROUP BY m.doc_id, m.sent_id
        )
        SELECT
            SUM(CASE WHEN n_codigos > 1 THEN 1 ELSE 0 END) AS multi,
            COUNT(*) AS total
        FROM frases;
    """
    df = ejecutar_sql(conn, sql)
    if df.empty:
        return

    multi = df.loc[0, "multi"]
    total = df.loc[0, "total"]

    df_res = pd.DataFrame([{
        "frases_multi": multi,
        "total": total,
        "porcentaje": (multi/total*100 if total else 0)
    }])
    guardar_csv(df_res, "pregunta09_porcentaje_multi")

    configurar_plt()
    plt.pie([multi, total - multi], labels=["≥2 sustancias", "1 sustancia"], autopct="%1.1f%%")
    plt.title("Porcentaje de frases multi-sustancia")
    ajustar_margenes()

    plt.savefig(os.path.join(FIG_DIR, "pregunta09.png"))
    print("[OK] pregunta09.png guardada")


def pregunta_10(conn):
    print("\n[10] F16 + frases de alarma...")
    sql = """
        SELECT entidad_norm, SUM(valor) AS total
        FROM fact_urgencias
        WHERE cie10_code LIKE 'F16%'
        GROUP BY entidad_norm
        ORDER BY total DESC;
    """
    df = ejecutar_sql(conn, sql)
    guardar_csv(df, "pregunta10_F16_entidades")

    if not df.empty:
        top = df.head(10)
        configurar_plt()
        plt.barh(top["entidad_norm"], top["total"])
        plt.gca().invert_yaxis()
        plt.title("Top entidades F16 (urgencias)")
        plt.xlabel("Urgencias")
        ajustar_margenes()
        plt.savefig(os.path.join(FIG_DIR, "pregunta10_entidades.png"))

    # Texto de alarma
    sql_txt = """
        SELECT sentence_norm
        FROM texto_frases
        WHERE cie10_code LIKE 'F16%'
        AND (
            sentence_norm LIKE '%grave%' OR
            sentence_norm LIKE '%urgente%' OR
            sentence_norm LIKE '%intoxicac%' OR
            sentence_norm LIKE '%riesgo%' OR
            sentence_norm LIKE '%coma%'
        );
    """
    df_txt = ejecutar_sql(conn, sql_txt)
    guardar_csv(df_txt, "pregunta10_F16_frases_alarma")
    print(f"[INFO] {len(df_txt)} frases de alarma encontradas.")


# =====================================================
# MAIN
# =====================================================

def main():
    conn = conectar()
    try:
        pregunta_1(conn)
        pregunta_2(conn)
        pregunta_3(conn)
        pregunta_4(conn)
        pregunta_5(conn)
        pregunta_6(conn)
        pregunta_7(conn)
        pregunta_8(conn)
        pregunta_9(conn)
        pregunta_10(conn)
    finally:
        conn.close()
        print("\n=== Consultas descriptivas completadas con éxito ===")


if __name__ == "__main__":
    main()
