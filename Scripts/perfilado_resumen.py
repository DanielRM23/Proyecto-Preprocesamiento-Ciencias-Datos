# Scripts/perfilado_datos.py
# Perfilado de datos para salud_federada.db
# Crea CSVs y un resumen Markdown con métricas de calidad (completitud, consistencia, duplicados, etc.)

import os, csv, sqlite3, datetime
DB = "salud_federada.db"
OUTDIR = "docs/Entrega4_perfilado"

def ensure_outdir():
    os.makedirs(OUTDIR, exist_ok=True)

def q(conn, sql, params=()):
    cur = conn.cursor()
    cur.execute(sql, params)
    cols = [c[0] for c in cur.description] if cur.description else []
    rows = cur.fetchall()
    return cols, rows

def save_csv(name, cols, rows):
    path = os.path.join(OUTDIR, name)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if cols: w.writerow(cols)
        w.writerows(rows)
    return path

def md_heading(f, text, level=2):
    f.write("\n" + "#"*level + f" {text}\n\n")

def main():
    ensure_outdir()
    conn = sqlite3.connect(DB)

    generated = []  # (title, path)

    # 0) Info general / tablas presentes
    cols, rows = q(conn, "SELECT name, type FROM sqlite_master WHERE type IN ('table','view') ORDER BY type, name;")
    path = save_csv("00_objetos_db.csv", cols, rows); generated.append(("Objetos en la DB", path))

    # 1) Conteos por tabla principal
    counts_sql = """
    SELECT 'fact_defunciones' AS tabla, COUNT(*) AS filas FROM fact_defunciones
    UNION ALL
    SELECT 'fact_urgencias', COUNT(*) FROM fact_urgencias
    UNION ALL
    SELECT 'cie10_nodes', COUNT(*) FROM cie10_nodes
    UNION ALL
    SELECT 'cie10_edges', COUNT(*) FROM cie10_edges
    UNION ALL
    SELECT 'texto_matches', COUNT(*) FROM texto_matches
    """
    cols, rows = q(conn, counts_sql)
    path = save_csv("01_conteos_basicos.csv", cols, rows); generated.append(("Conteos básicos", path))

    # 2) Rango de años y conteos por fuente en v_eventos
    cols, rows = q(conn, "SELECT MIN(anio) AS min_anio, MAX(anio) AS max_anio FROM v_eventos;")
    path = save_csv("02_rango_anios.csv", cols, rows); generated.append(("Rango de años", path))
    cols, rows = q(conn, "SELECT fuente, COUNT(*) AS filas, SUM(valor) AS total FROM v_eventos GROUP BY fuente;")
    path = save_csv("03_eventos_por_fuente.csv", cols, rows); generated.append(("Eventos por fuente", path))

    # 3) Completitud por origen en v_unificado (porcentaje de no-nulos)
    comp_sql = """
    WITH base AS (
      SELECT origen,
             (cie10_code IS NOT NULL) AS ok_code,
             (texto IS NOT NULL)      AS ok_texto,
             (anio IS NOT NULL)       AS ok_anio,
             (entidad_norm IS NOT NULL) AS ok_entidad,
             (sexo IS NOT NULL)       AS ok_sexo,
             (valor IS NOT NULL)      AS ok_valor
      FROM v_unificado
    )
    SELECT origen,
           ROUND(100.0 * AVG(ok_code),2)   AS pct_cie10_code,
           ROUND(100.0 * AVG(ok_texto),2)  AS pct_texto,
           ROUND(100.0 * AVG(ok_anio),2)   AS pct_anio,
           ROUND(100.0 * AVG(ok_entidad),2) AS pct_entidad,
           ROUND(100.0 * AVG(ok_sexo),2)   AS pct_sexo,
           ROUND(100.0 * AVG(ok_valor),2)  AS pct_valor,
           COUNT(*) AS filas
    FROM base
    GROUP BY origen
    ORDER BY origen;
    """
    cols, rows = q(conn, comp_sql)
    path = save_csv("04_completitud_por_origen.csv", cols, rows); generated.append(("Completitud por origen", path))

    # 4) CIE-10 inválidos o fuera de rango F10–F19
    cols, rows = q(conn, """
        SELECT DISTINCT cie10_code
        FROM v_unificado
        WHERE cie10_code IS NOT NULL
          AND cie10_code NOT GLOB 'F1[0-9]*'
        ORDER BY cie10_code;
    """)
    path = save_csv("05_cie10_fuera_de_rango.csv", cols, rows); generated.append(("CIE-10 fuera de rango", path))

    # 5) Códigos en texto/SQL que no existen en el grafo (huérfanos)
    cols, rows = q(conn, """
        SELECT 'texto' AS fuente, t.cie10_code
        FROM texto_matches t
        LEFT JOIN cie10_nodes n USING (cie10_code)
        WHERE t.cie10_code IS NOT NULL AND n.cie10_code IS NULL
        UNION
        SELECT 'sql' AS fuente, e.cie10_code
        FROM v_eventos e
        LEFT JOIN cie10_nodes n USING (cie10_code)
        WHERE e.cie10_code IS NOT NULL AND n.cie10_code IS NULL
        ORDER BY fuente, cie10_code;
    """)
    path = save_csv("06_codigos_huerfanos_vs_grafo.csv", cols, rows); generated.append(("Códigos huérfanos", path))

    # 6) Duplicados potenciales en hechos y en texto
    cols, rows = q(conn, """
        SELECT anio, entidad_norm, sexo, cie10_code, COUNT(*) AS veces
        FROM fact_defunciones
        GROUP BY anio, entidad_norm, sexo, cie10_code
        HAVING COUNT(*) > 1
        ORDER BY veces DESC, anio DESC
        LIMIT 200;
    """)
    path = save_csv("07_dup_defunciones.csv", cols, rows); generated.append(("Duplicados defunciones", path))

    cols, rows = q(conn, """
        SELECT anio, entidad_norm, sexo, cie10_code, COUNT(*) AS veces
        FROM fact_urgencias
        GROUP BY anio, entidad_norm, sexo, cie10_code
        HAVING COUNT(*) > 1
        ORDER BY veces DESC, anio DESC
        LIMIT 200;
    """)
    path = save_csv("08_dup_urgencias.csv", cols, rows); generated.append(("Duplicados urgencias", path))

    cols, rows = q(conn, """
        SELECT cie10_code, sentence, COUNT(*) AS veces
        FROM texto_matches
        GROUP BY cie10_code, sentence
        HAVING COUNT(*) > 1
        ORDER BY veces DESC
        LIMIT 200;
    """)
    path = save_csv("09_dup_texto_matches.csv", cols, rows); generated.append(("Duplicados texto", path))

    # 7) Valores no esperados en sexo y rango de años atípicos
    cols, rows = q(conn, """
        SELECT 'defunciones' AS fuente, sexo, COUNT(*) AS filas
        FROM fact_defunciones
        GROUP BY sexo
        UNION ALL
        SELECT 'urgencias', sexo, COUNT(*)
        FROM fact_urgencias
        GROUP BY sexo;
    """)
    path = save_csv("10_distribucion_sexo.csv", cols, rows); generated.append(("Distribución de sexo", path))

    cols, rows = q(conn, """
        SELECT MIN(anio) AS min_anio, MAX(anio) AS max_anio
        FROM (
          SELECT anio FROM fact_defunciones
          UNION ALL
          SELECT anio FROM fact_urgencias
        );
    """)
    path = save_csv("11_rango_anios_fact.csv", cols, rows); generated.append(("Rango de años (fact)", path))

    # 8) Cobertura texto↔SQL (para relevancia/consistencia)
    cols, rows = q(conn, """
        WITH cod_texto AS (SELECT DISTINCT cie10_code FROM texto_matches WHERE cie10_code IS NOT NULL),
             cod_sql AS (SELECT DISTINCT cie10_code FROM v_eventos WHERE cie10_code IS NOT NULL)
        SELECT
          (SELECT COUNT(*) FROM cod_texto) AS codigos_texto,
          (SELECT COUNT(*) FROM cod_sql)   AS codigos_sql,
          (SELECT COUNT(*) FROM cod_texto t INNER JOIN cod_sql s USING(cie10_code)) AS interseccion;
    """)
    path = save_csv("12_cobertura_texto_sql.csv", cols, rows); generated.append(("Cobertura texto↔SQL", path))

    # 9) Top entidades por código (ejemplo de patrón/consistencia)
    cols, rows = q(conn, """
        SELECT cie10_code, entidad_norm, SUM(valor) AS total
        FROM v_eventos
        GROUP BY cie10_code, entidad_norm
        ORDER BY total DESC
        LIMIT 50;
    """)
    path = save_csv("13_top_entidad_por_codigo.csv", cols, rows); generated.append(("Top entidad por código", path))

    # 10) Resumen por origen en v_unificado (cantidad de filas)
    cols, rows = q(conn, "SELECT origen, COUNT(*) AS filas FROM v_unificado GROUP BY origen ORDER BY filas DESC;")
    path = save_csv("14_filas_por_origen.csv", cols, rows); generated.append(("Filas por origen (v_unificado)", path))

    conn.close()

    # ---- Markdown de resumen ----
    md_path = os.path.join(OUTDIR, "perfilado_resumen.md")
    with open(md_path, "w", encoding="utf-8") as f:
        f.write(f"# Perfilado de Datos – Salud Federada\n\n")
        f.write(f"_Generado: {datetime.datetime.now().isoformat(timespec='seconds')}_\n\n")
        md_heading(f, "Objetos en la base")
        f.write("- Tablas y vistas listadas en `00_objetos_db.csv`.\n")

        md_heading(f, "Volumen por fuente")
        f.write("- Conteos: `01_conteos_basicos.csv`.\n")
        f.write("- Rango de años (hechos): `02_rango_anios.csv`, `11_rango_anios_fact.csv`.\n")
        f.write("- Eventos por fuente (defunciones/urgencias): `03_eventos_por_fuente.csv`.\n")

        md_heading(f, "Calidad: Completitud y Consistencia")
        f.write("- Completitud por origen (no-nulos por columna): `04_completitud_por_origen.csv`.\n")
        f.write("- CIE-10 fuera de F10–F19: `05_cie10_fuera_de_rango.csv`.\n")
        f.write("- Códigos huérfanos respecto al grafo: `06_codigos_huerfanos_vs_grafo.csv`.\n")

        md_heading(f, "Duplicados")
        f.write("- Defunciones: `07_dup_defunciones.csv`.\n")
        f.write("- Urgencias: `08_dup_urgencias.csv`.\n")
        f.write("- Texto (sentence, cie10_code): `09_dup_texto_matches.csv`.\n")

        md_heading(f, "Validaciones adicionales")
        f.write("- Distribución de `sexo`: `10_distribucion_sexo.csv`.\n")
        f.write("- Cobertura texto↔SQL (intersección de códigos): `12_cobertura_texto_sql.csv`.\n")
        f.write("- Top entidades por código (para detectar patrones): `13_top_entidad_por_codigo.csv`.\n")
        f.write("- Filas por origen en la vista unificada: `14_filas_por_origen.csv`.\n")

        md_heading(f, "Notas para el informe")
        f.write("""- Los NULL en `v_unificado` son **esperados** por heterogeneidad (p.ej., en filas de texto no aplica `valor`; en filas SQL no hay `texto`).
- `cie10_code` es la **clave semántica** común que permite integrar grafo, texto y SQL.
- Los duplicados listados son **candidatos** para limpieza (de-duplicación); si alguno es legítimo por diseño, debe documentarse la regla.
- Para “forzar patrones”, normalizar `cie10_code` a mayúsculas, `sexo` a {Masculino,Femenino,No Especificado} y `entidad_norm` según catálogo.\n""")

    print(f"[OK] Perfilado generado en: {OUTDIR}")
    print(f"  - Resumen: {md_path}")
    for title, path in generated:
        print(f"  - {title}: {path}")

if __name__ == "__main__":
    main()
