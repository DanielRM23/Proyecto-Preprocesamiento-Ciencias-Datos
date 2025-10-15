#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
crear_vistas.py
Crea/actualiza las vistas para la base 'salud_federada.db' con el esquema final:
- sin cve_entidad en hechos
- sin weight en edges
- texto desde texto_frases (+texto_frases_x_docs) o, si no existe, desde texto_matches (fallback)
"""

import sqlite3

DB_PATH = "salud_federada.db"

SQL_COMMON = """
-- =================== v_eventos ===================
DROP VIEW IF EXISTS v_eventos;
CREATE VIEW v_eventos AS
SELECT anio, entidad_norm, edad_quinquenal, sexo, cie10_code, valor, 'defunciones' AS fuente
  FROM fact_defunciones
UNION ALL
SELECT anio, entidad_norm, edad_quinquenal, sexo, cie10_code, valor, 'urgencias'   AS fuente
  FROM fact_urgencias;

-- ============== v_eventos_por_codigo ==============
DROP VIEW IF EXISTS v_eventos_por_codigo;
CREATE VIEW v_eventos_por_codigo AS
SELECT cie10_code, fuente, SUM(valor) AS total
FROM v_eventos
GROUP BY cie10_code, fuente;

-- ======= v_relaciones_epidemiologicas (sin weight) =======
DROP VIEW IF EXISTS v_relaciones_epidemiologicas;
CREATE VIEW v_relaciones_epidemiologicas AS
SELECT source AS cie10_a, target AS cie10_b, rel_type AS tipo
FROM cie10_edges
WHERE rel_type IS NOT NULL;
"""

# Texto limpio (catálogo de frases + mapeo doc/sent)
SQL_TEXTO_FRASES = """
-- =================== v_texto_por_codigo ===================
DROP VIEW IF EXISTS v_texto_por_codigo;
CREATE VIEW v_texto_por_codigo AS
SELECT cie10_code, sentence_norm, n_ocurrencias
FROM texto_frases;

-- =================== v_unificado ===================
DROP VIEW IF EXISTS v_unificado;
CREATE VIEW v_unificado AS

-- GRAFO
SELECT
  'grafo' AS origen,
  printf('G:%s', n.code) AS id_origen,
  n.code       AS cie10_code,
  NULL         AS anio,
  NULL         AS entidad_norm,
  NULL         AS edad_quinquenal,
  NULL         AS sexo,
  NULL         AS valor,
  NULL         AS fuente,
  n.descripcion AS texto,
  'cie10_nodes.descripcion' AS campo
FROM cie10_nodes n

UNION ALL
-- TEXTO (frases + mapeo doc/sent si existe)
SELECT
  'texto' AS origen,
  CASE
    WHEN m.doc_id IS NOT NULL AND m.sent_id IS NOT NULL
      THEN printf('T:%d:%d', m.doc_id, m.sent_id)
    ELSE printf('T:%s', f.phrase_hash)
  END AS id_origen,
  f.cie10_code,
  NULL AS anio,
  NULL AS entidad_norm,
  NULL AS edad_quinquenal,
  NULL AS sexo,
  NULL AS valor,
  'cowese' AS fuente,
  f.sentence_norm AS texto,
  'texto_frases.sentence_norm' AS campo
FROM texto_frases f
LEFT JOIN texto_frases_x_docs m
  ON m.phrase_hash = f.phrase_hash

UNION ALL
-- SQL DEFUNCIONES
SELECT
  'sql_defunciones' AS origen,
  printf('D:%d:%s:%s:%s',
         COALESCE(d.anio,-1),
         COALESCE(d.entidad_norm,'?'),
         COALESCE(d.edad_quinquenal,'?'),
         COALESCE(d.sexo,'?')
  ) AS id_origen,
  d.cie10_code,
  d.anio,
  d.entidad_norm,
  d.edad_quinquenal,
  d.sexo,
  d.valor,
  d.fuente,
  NULL AS texto,
  NULL AS campo
FROM fact_defunciones d

UNION ALL
-- SQL URGENCIAS
SELECT
  'sql_urgencias' AS origen,
  printf('U:%d:%s:%s:%s',
         COALESCE(u.anio,-1),
         COALESCE(u.entidad_norm,'?'),
         COALESCE(u.edad_quinquenal,'?'),
         COALESCE(u.sexo,'?')
  ) AS id_origen,
  u.cie10_code,
  u.anio,
  u.entidad_norm,
  u.edad_quinquenal,
  u.sexo,
  u.valor,
  u.fuente,
  NULL AS texto,
  NULL AS campo
FROM fact_urgencias u;
"""

# Texto crudo (fallback a texto_matches)
SQL_TEXTO_MATCHES = """
-- =================== v_eventos_con_texto (fallback) ===================
DROP VIEW IF EXISTS v_eventos_con_texto;
CREATE VIEW v_eventos_con_texto AS
SELECT e.anio, e.entidad_norm, e.cie10_code, e.valor, e.fuente,
       t.doc_id, t.sent_id, t.keyword, t.sentence
FROM v_eventos e
LEFT JOIN texto_matches t USING (cie10_code);

-- =================== v_unificado (fallback con matches) ===================
DROP VIEW IF EXISTS v_unificado;
CREATE VIEW v_unificado AS

-- GRAFO
SELECT
  'grafo' AS origen,
  printf('G:%s', n.code) AS id_origen,
  n.code       AS cie10_code,
  NULL         AS anio,
  NULL         AS entidad_norm,
  NULL         AS edad_quinquenal,
  NULL         AS sexo,
  NULL         AS valor,
  NULL         AS fuente,
  n.descripcion AS texto,
  'cie10_nodes.descripcion' AS campo
FROM cie10_nodes n

UNION ALL
-- TEXTO (matches crudos)
SELECT
  'texto' AS origen,
  printf('T:%d:%d', COALESCE(t.doc_id,-1), COALESCE(t.sent_id,-1)) AS id_origen,
  t.cie10_code,
  t.anio,
  t.cve_entidad AS entidad_norm,   -- si no existe será NULL
  NULL AS edad_quinquenal,
  NULL AS sexo,
  NULL AS valor,
  'cowese' AS fuente,
  t.sentence AS texto,
  'texto_matches.sentence' AS campo
FROM texto_matches t

UNION ALL
-- SQL DEFUNCIONES
SELECT
  'sql_defunciones' AS origen,
  printf('D:%d:%s:%s:%s',
         COALESCE(d.anio,-1),
         COALESCE(d.entidad_norm,'?'),
         COALESCE(d.edad_quinquenal,'?'),
         COALESCE(d.sexo,'?')
  ) AS id_origen,
  d.cie10_code,
  d.anio,
  d.entidad_norm,
  d.edad_quinquenal,
  d.sexo,
  d.valor,
  d.fuente,
  NULL AS texto,
  NULL AS campo
FROM fact_defunciones d

UNION ALL
-- SQL URGENCIAS
SELECT
  'sql_urgencias' AS origen,
  printf('U:%d:%s:%s:%s',
         COALESCE(u.anio,-1),
         COALESCE(u.entidad_norm,'?'),
         COALESCE(u.edad_quinquenal,'?'),
         COALESCE(u.sexo,'?')
  ) AS id_origen,
  u.cie10_code,
  u.anio,
  u.entidad_norm,
  u.edad_quinquenal,
  u.sexo,
  u.valor,
  u.fuente,
  NULL AS texto,
  NULL AS campo
FROM fact_urgencias u;
"""

def table_exists(cur, name: str) -> bool:
    row = cur.execute(
        "SELECT 1 FROM sqlite_master WHERE type IN ('table','view') AND name=?;", (name,)
    ).fetchone()
    return row is not None

def main():
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()

    # Siempre crear las vistas comunes
    cur.executescript(SQL_COMMON)

    # Elegir bloque de texto según lo disponible
    has_frases = table_exists(cur, "texto_frases")
    has_map    = table_exists(cur, "texto_frases_x_docs")
    has_match  = table_exists(cur, "texto_matches")

    if has_frases:
        cur.executescript(SQL_TEXTO_FRASES)
        print("[OK] Vistas creadas usando catálogo de texto (texto_frases).")
    elif has_match:
        cur.executescript(SQL_TEXTO_MATCHES)
        print("[OK] Vistas creadas usando texto_matches (fallback).")
    else:
        # Sin texto: crea v_unificado sólo con grafo + hechos
        cur.executescript("""
        DROP VIEW IF EXISTS v_unificado;
        CREATE VIEW v_unificado AS

        -- GRAFO
        SELECT
          'grafo' AS origen,
          printf('G:%s', n.code) AS id_origen,
          n.code       AS cie10_code,
          NULL         AS anio,
          NULL         AS entidad_norm,
          NULL         AS edad_quinquenal,
          NULL         AS sexo,
          NULL         AS valor,
          NULL         AS fuente,
          n.descripcion AS texto,
          'cie10_nodes.descripcion' AS campo
        FROM cie10_nodes n

        UNION ALL
        -- SQL DEFUNCIONES
        SELECT
          'sql_defunciones' AS origen,
          printf('D:%d:%s:%s:%s',
                 COALESCE(d.anio,-1),
                 COALESCE(d.entidad_norm,'?'),
                 COALESCE(d.edad_quinquenal,'?'),
                 COALESCE(d.sexo,'?')
          ) AS id_origen,
          d.cie10_code,
          d.anio,
          d.entidad_norm,
          d.edad_quinquenal,
          d.sexo,
          d.valor,
          d.fuente,
          NULL AS texto,
          NULL AS campo
        FROM fact_defunciones d

        UNION ALL
        -- SQL URGENCIAS
        SELECT
          'sql_urgencias' AS origen,
          printf('U:%d:%s:%s:%s',
                 COALESCE(u.anio,-1),
                 COALESCE(u.entidad_norm,'?'),
                 COALESCE(u.edad_quinquenal,'?'),
                 COALESCE(u.sexo,'?')
          ) AS id_origen,
          u.cie10_code,
          u.anio,
          u.entidad_norm,
          u.edad_quinquenal,
          u.sexo,
          u.valor,
          u.fuente,
          NULL AS texto,
          NULL AS campo
        FROM fact_urgencias u;
        """)
        print("[OK] Vistas creadas sin componente de texto (no se encontraron tablas de texto).")

    con.commit()

    # Pruebas rápidas
    print("\n[CHECK] .tables")
    for (name,) in cur.execute("SELECT name FROM sqlite_master WHERE type='view' ORDER BY name;"):
        print("  ", name)

    print("\n[CHECK] Orígenes en v_unificado")
    try:
        for row in cur.execute("SELECT origen, COUNT(*) FROM v_unificado GROUP BY origen;"):
            print("  ", row)
    except Exception as e:
        print("  (no disponible)", e)

    con.close()
    print("\n[OK] Vistas creadas/actualizadas.")

if __name__ == "__main__":
    main()
