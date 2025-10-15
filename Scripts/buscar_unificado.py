#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
buscar_unificado.py
Buscador con procedencia sobre la vista v_unificado de salud_federada.db (esquema final)

Funciones:
- Búsqueda textual (FTS5 unicode61) sobre grafo/texto con indicación de origen.
- Resumen por origen (--top-origen) para una query o un código CIE-10.
- Ejemplos SQL de eventos (defunciones/urgencias) para un código (--ejemplos-sql).
- Fallback sin FTS (--no-fts) usando LIKE sobre v_unificado.
- Reconstrucción de índice FTS (--rebuild-fts).
- Exportar resultados actuales a CSV (--export-csv archivo.csv).
- Exportar coincidencias de texto desde texto_frases o texto_matches (--export-texto archivo.csv).
- Exportar los 3 subconjuntos a la vez (--export-all PREFIX).
"""

import argparse
import csv
import sqlite3

DB_PATH = "salud_federada.db"

SQL_CREATE_FTS_TEMPLATE = """
-- Reset
DROP TABLE IF EXISTS fts_conocimiento;

CREATE VIRTUAL TABLE fts_conocimiento USING fts5(
  origen,          -- 'grafo' | 'texto'
  cie10_code,
  campo,           -- de qué columna proviene el texto
  texto,
  tokenize = 'unicode61'
);

/* 1) Grafo: descripciones de CIE-10 (cie10_nodes.code) */
INSERT INTO fts_conocimiento (origen, cie10_code, campo, texto)
SELECT 'grafo' AS origen,
       n.code              AS cie10_code,
       'cie10_nodes.descripcion' AS campo,
       n.descripcion       AS texto
FROM cie10_nodes n
WHERE n.descripcion IS NOT NULL AND n.descripcion <> '';
"""

SQL_CREATE_FTS_TEXTO_FRASES = """
/* 2) Texto (limpio): texto_frases.sentence_norm */
INSERT INTO fts_conocimiento (origen, cie10_code, campo, texto)
SELECT 'texto' AS origen,
       f.cie10_code       AS cie10_code,
       'texto_frases.sentence_norm' AS campo,
       f.sentence_norm    AS texto
FROM texto_frases f
WHERE f.sentence_norm IS NOT NULL AND f.sentence_norm <> '';
"""

SQL_CREATE_FTS_TEXTO_MATCHES = """
/* 2) Texto (crudo fallback): texto_matches.sentence */
INSERT INTO fts_conocimiento (origen, cie10_code, campo, texto)
SELECT 'texto' AS origen,
       t.cie10_code        AS cie10_code,
       'texto_matches.sentence' AS campo,
       t.sentence          AS texto
FROM texto_matches t
WHERE t.sentence IS NOT NULL AND t.sentence <> '';
"""

def table_exists(conn, name: str) -> bool:
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM sqlite_master WHERE type IN ('table','view') AND name=?;", (name,))
    return cur.fetchone() is not None

def ensure_fts(conn, force_rebuild: bool = False):
    cur = conn.cursor()
    if (not force_rebuild):
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='fts_conocimiento';")
        if cur.fetchone() is not None:
            return

    # Crear base del índice (grafo)
    cur.executescript(SQL_CREATE_FTS_TEMPLATE)

    # Añadir texto desde la mejor fuente disponible
    if table_exists(conn, "texto_frases"):
        cur.executescript(SQL_CREATE_FTS_TEXTO_FRASES)
    elif table_exists(conn, "texto_matches"):
        cur.executescript(SQL_CREATE_FTS_TEXTO_MATCHES)
    else:
        # no hay componente de texto; FTS solo para grafo
        pass

    conn.commit()

def buscar(conn, query=None, origen=None, cie10=None, limit=30, use_fts=True):
    """
    Devuelve filas (origen, cie10_code, snippet_texto)
    - Con FTS: consulta fts_conocimiento (rápido, acentos OK).
    - Sin FTS: consulta v_unificado con LIKE (más lento).
    """
    cur = conn.cursor()
    params = []

    if use_fts and query:
        ensure_fts(conn)
        sql = """
        SELECT COALESCE(origen,'-') AS origen,
               COALESCE(cie10_code,'-') AS cie10_code,
               substr(texto,1,200) AS snippet
        FROM fts_conocimiento
        WHERE fts_conocimiento MATCH ?
        """
        params.append(query)
        if origen:
            sql += " AND origen = ?"
            params.append(origen)
        if cie10:
            sql += " AND cie10_code = ?"
            params.append(cie10)
        sql += " LIMIT ?"
        params.append(limit)
    else:
        sql = """
        SELECT COALESCE(origen,'-') AS origen,
               COALESCE(cie10_code,'-') AS cie10_code,
               substr(texto,1,200) AS snippet
        FROM v_unificado
        WHERE texto IS NOT NULL
        """
        if query:
            sql += " AND texto LIKE '%' || ? || '%'"
            params.append(query)
        if origen:
            sql += " AND origen = ?"
            params.append(origen)
        if cie10:
            sql += " AND cie10_code = ?"
            params.append(cie10)
        sql += " LIMIT ?"
        params.append(limit)

    cur.execute(sql, params)
    return cur.fetchall()

def top_por_origen(conn, codigo=None, query=None, limit=10):
    """
    Resumen de conteos por origen.
    - Si 'codigo' está definido: cuenta en v_unificado por cie10_code.
    - Si 'query' está definida: cuenta en fts_conocimiento por término.
    """
    cur = conn.cursor()
    if codigo:
        sql = """
        SELECT COALESCE(origen,'-') AS origen, COUNT(*) AS n
        FROM v_unificado
        WHERE cie10_code = ?
        GROUP BY origen
        ORDER BY n DESC
        LIMIT ?
        """
        cur.execute(sql, (codigo, limit))
        return cur.fetchall()
    elif query:
        ensure_fts(conn)
        sql = """
        SELECT COALESCE(origen,'-') AS origen, COUNT(*) AS n
        FROM fts_conocimiento
        WHERE fts_conocimiento MATCH ?
        GROUP BY origen
        ORDER BY n DESC
        LIMIT ?
        """
        cur.execute(sql, (query, limit))
        return cur.fetchall()
    else:
        return []

def ejemplos_sql(conn, codigo="F14", limit=10):
    """
    Devuelve filas de eventos SQL (defunciones/urgencias) con mayor 'valor' para un código dado.
    Columnas: origen, anio, entidad_norm, edad_quinquenal, sexo, valor, fuente
    """
    cur = conn.cursor()
    sql = """
    SELECT COALESCE(origen,'-') AS origen,
           anio,
           COALESCE(entidad_norm,'-') AS entidad_norm,
           COALESCE(edad_quinquenal,'-') AS edad_quinquenal,
           COALESCE(sexo,'-') AS sexo,
           COALESCE(valor,0) AS valor,
           COALESCE(fuente,'-') AS fuente
    FROM v_unificado
    WHERE origen IN ('sql_defunciones','sql_urgencias')
      AND cie10_code = ?
    ORDER BY valor DESC
    LIMIT ?
    """
    cur.execute(sql, (codigo, limit))
    return cur.fetchall()

def export_grafo(conn, term, outfile):
    """
    Exporta descripciones del grafo usando FTS (origen='grafo').
    Columnas: cie10_code, descripcion
    """
    ensure_fts(conn)  # garantizamos que exista
    sql = """
    SELECT cie10_code, texto AS descripcion
    FROM fts_conocimiento
    WHERE origen='grafo' AND fts_conocimiento MATCH ?
    """
    rows = conn.execute(sql, (term,)).fetchall()
    import csv
    with open(outfile, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["cie10_code","descripcion"])
        w.writerows(rows)
    return len(rows)


def export_texto(conn, term, outfile):
    """
    Exporta coincidencias de TEXTO usando FTS (origen='texto').
    Si existe texto_frases, mantenemos sentence_norm; si no, usamos el texto FTS.
    Columnas preferidas: cie10_code, sentence_norm (o texto), n_ocurrencias (si disponible)
    """
    ensure_fts(conn)
    cur = conn.cursor()

    # 1) Trae las coincidencias desde FTS (texto)
    fts_rows = cur.execute("""
        SELECT cie10_code, texto
        FROM fts_conocimiento
        WHERE origen='texto' AND fts_conocimiento MATCH ?
    """, (term,)).fetchall()

    if not fts_rows:
        # nada que exportar
        with open(outfile, "w", newline="", encoding="utf-8") as f:
            import csv; csv.writer(f).writerow(["cie10_code","sentence","n_ocurrencias"])
        return 0

    # 2) Si hay texto_frases, intentamos mapear por sentencia normalizada aproximando por igualdad;
    #    si no, exportamos directo lo de FTS.
    has_frases = bool(cur.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='texto_frases'").fetchone())

    import csv
    with open(outfile, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if has_frases:
            # Normalizamos el texto FTS a lowercase & sin espacios extra para comparar
            import re, unicodedata
            def norm(s):
                s = "".join(c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c))
                s = s.lower()
                s = re.sub(r"\s+", " ", s).strip()
                return s

            # cargamos un mini diccionario sentence_norm -> (cie10_code, n_ocurrencias)
            frases = cur.execute("SELECT cie10_code, sentence_norm, n_ocurrencias FROM texto_frases").fetchall()
            idx = {}
            for code, sent_norm, n in frases:
                if sent_norm: idx[norm(sent_norm)] = (code, sent_norm, n or 1)

            # Hacemos match best-effort; si no encontramos, exportamos el texto FTS sin n_ocurrencias
            w.writerow(["cie10_code","sentence_norm","n_ocurrencias"])
            out = 0
            for code, sent in fts_rows:
                key = norm(sent or "")
                if key in idx:
                    c, s, n = idx[key]
                    w.writerow([c, s, n])
                else:
                    w.writerow([code, sent, ""])
                out += 1
            return out
        else:
            # Sin catálogo de frases: volcamos lo del FTS
            w.writerow(["cie10_code","sentence"])
            w.writerows(fts_rows)
            return len(fts_rows)


def export_sql_por_term(conn, term, outfile):
    """
    Detecta códigos por FTS (texto) y exporta eventos SQL desde v_unificado.
    Columnas: origen, anio, entidad_norm, edad_quinquenal, sexo, valor, fuente, cie10_code
    """
    ensure_fts(conn)
    cur = conn.cursor()
    # códigos desde FTS (texto)
    codigos = [r[0] for r in cur.execute("""
        SELECT DISTINCT cie10_code
        FROM fts_conocimiento
        WHERE origen='texto' AND fts_conocimiento MATCH ?
    """, (term,)).fetchall()]

    import csv
    with open(outfile, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["origen","anio","entidad_norm","edad_quinquenal","sexo","valor","fuente","cie10_code"])
        if not codigos:
            return 0
        placeholders = ",".join("?" * len(codigos))
        sql = f"""
        SELECT origen, anio, entidad_norm, edad_quinquenal, sexo, valor, fuente, cie10_code
        FROM v_unificado
        WHERE origen IN ('sql_defunciones','sql_urgencias')
          AND cie10_code IN ({placeholders})
        ORDER BY valor DESC, anio DESC
        """
        rows = cur.execute(sql, codigos).fetchall()
        w.writerows(rows)
        return len(rows)


def export_all(conn, term, prefix):
    """
    Genera:
      <prefix>_grafo.csv
      <prefix>_texto.csv
      <prefix>_sql.csv
    """
    n_g = export_grafo(conn, term, f"{prefix}_grafo.csv")
    n_t = export_texto(conn, term, f"{prefix}_texto.csv")
    n_s = export_sql_por_term(conn, term, f"{prefix}_sql.csv")
    return n_g, n_t, n_s


def _variants_for(term: str):
    t = term.strip().lower()
    if t in {"cannabis", "marihuana", "mariguana"}:
        return ["cannab*", "marihuan*", "mariguan*", "thc", "tetrahidrocannabinol"]
    if t in {"cocaína", "cocaina", "cocaine"}:
        return ["cocain*", "cocaín*", "crack"]
    if t in {"alcohol", "etanol"}:
        return ["alcohol", "etanol", "bebidas alcoh*"]
    if t in {"opioides", "opiaceos", "opiáceos"}:
        return ["opioid*", "opiace*", "morfina", "heroina", "fentanil*"]
    if t in {"anfetaminas", "estimulantes"}:
        return ["anfetamin*", "metanfetamin*", "estimulant*"]
    # fallback: intenta el prefijo
    if len(t) >= 4:
        return [t + "*"]
    return [t]

def _fts_union_rows(conn, origin_filter, terms):
    # Une resultados FTS de varios términos y quita duplicados
    seen = set(); out = []
    for q in terms:
        for row in conn.execute("""
            SELECT origen, cie10_code, texto
            FROM fts_conocimiento
            WHERE origen=? AND fts_conocimiento MATCH ?
        """, (origin_filter, q)).fetchall():
            key = (row[0], row[1], row[2])
            if key not in seen:
                seen.add(key); out.append(row)
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("query", nargs="?", default=None, help="Término a buscar en texto/descripciones (FTS/LIKE)")
    ap.add_argument("--origen", choices=["grafo","texto","sql_defunciones","sql_urgencias"], help="Filtrar por origen")
    ap.add_argument("--cie10", "--codigo", dest="cie10", help="Filtrar por código CIE-10 (ej. F14)")
    ap.add_argument("--limit", type=int, default=20, help="Máx. resultados a mostrar")
    ap.add_argument("--no-fts", action="store_true", help="No usar FTS5 (usa LIKE en v_unificado)")
    ap.add_argument("--top-origen", action="store_true",
                    help="Resumen de conteos por origen (requiere --codigo o query)")
    ap.add_argument("--ejemplos-sql", action="store_true", help="Muestra top filas SQL para --codigo (default F14)")
    ap.add_argument("--rebuild-fts", action="store_true", help="Reconstruir el índice FTS antes de consultar")
    ap.add_argument("--export-csv", metavar="FILENAME",
                    help="Exportar resultados de la búsqueda actual (origen, cie10_code, snippet) a CSV")
    ap.add_argument("--export-texto", metavar="FILENAME",
                    help="Exportar coincidencias desde texto_frases o texto_matches (requiere query)")
    ap.add_argument("--export-all", metavar="PREFIX",
                    help="Exporta 3 CSV: <PREFIX>_grafo.csv, <PREFIX>_texto.csv, <PREFIX>_sql.csv")
    ap.add_argument("--expand", action="store_true",
                    help="Expandir sinónimos/variantes comunes (p.ej. cannabis → cannab*, marihuan*, THC)")

    args = ap.parse_args()
    conn = sqlite3.connect(DB_PATH)

    # Reconstruir FTS si se pide
    if args.rebuild_fts:
        ensure_fts(conn, force_rebuild=True)

    # Exportación directa del componente de texto (texto_frases o texto_matches)
    if args.export_texto:
        if not args.query:
            print("[ERROR] --export-texto requiere una 'query' (término a buscar).")
            conn.close()
            return
        n = export_texto(conn, args.query, args.export_texto)
        print(f"[OK] Exportadas {n} filas a {args.export_texto}")
        conn.close()
        return

    # Resumen por origen
    if args.top_origen:
        rows = top_por_origen(conn, codigo=args.cie10, query=args.query, limit=10)
        print("\n== Top por origen ==")
        for origen, n in rows:
            print(f"{(origen or '-'):16s}  {n or 0}")
        if not rows:
            print("(sin resultados)")
        conn.close()
        return

    # Ejemplos SQL para un código
    if args.ejemplos_sql:
        codigo = args.cie10 or "F14"
        rows = ejemplos_sql(conn, codigo=codigo, limit=args.limit)
        print(f"\n== Ejemplos SQL para {codigo} ==")
        for origen, anio, ent, edad, sexo, valor, fuente in rows:
            print(f"{origen:16s}  {anio}  {ent:20.20s} {edad:10.10s} {sexo:10.10s}  valor={valor:6d}  fuente={fuente}")
        if not rows:
            print("(sin resultados)")
        conn.close()
        return

    # Exportación en un solo tiro (grafo, texto, sql) usando FTS (+ expansión opcional)
    if args.export_all:
        if not args.query:
            print("[ERROR] --export-all requiere una 'query'.")
            conn.close()
            return
        ensure_fts(conn)
        terms = _variants_for(args.query) if args.expand else [args.query]

        # GRAFO (desde FTS)
        grafo_rows = []
        for q in terms:
            grafo_rows += conn.execute("""
                SELECT cie10_code, texto AS descripcion
                FROM fts_conocimiento
                WHERE origen='grafo' AND fts_conocimiento MATCH ?
            """, (q,)).fetchall()
        # deduplicar
        g_seen, g_out = set(), []
        for r in grafo_rows:
            if r not in g_seen:
                g_seen.add(r)
                g_out.append(r)

        # TEXTO (desde FTS, uniendo variantes)
        texto_rows = _fts_union_rows(conn, "texto", terms)

        # SQL por códigos detectados en TEXTO
        codes = sorted({r[1] for r in texto_rows if r[1]})
        sql_rows = []
        if codes:
            ph = ",".join("?" * len(codes))
            sql_rows = conn.execute(f"""
                SELECT origen, anio, entidad_norm, edad_quinquenal, sexo, valor, fuente, cie10_code
                FROM v_unificado
                WHERE origen IN ('sql_defunciones','sql_urgencias')
                  AND cie10_code IN ({ph})
                ORDER BY valor DESC, anio DESC
            """, codes).fetchall()

        # Guardar CSVs
        with open(f"{args.export_all}_grafo.csv", "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f); w.writerow(["cie10_code","descripcion"]); w.writerows(g_out)
        with open(f"{args.export_all}_texto.csv", "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f); w.writerow(["origen","cie10_code","sentence"]); w.writerows(texto_rows)
        with open(f"{args.export_all}_sql.csv", "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f); w.writerow(["origen","anio","entidad_norm","edad_quinquenal","sexo","valor","fuente","cie10_code"]); w.writerows(sql_rows)

        print(f"[OK] Exportados: grafo={len(g_out)}, texto={len(texto_rows)}, sql={len(sql_rows)} → {args.export_all}_*.csv")
        conn.close()
        return

    # Búsqueda normal (FTS o LIKE) y export opcional
    rows = buscar(conn,
                  query=args.query,
                  origen=args.origen,
                  cie10=args.cie10,
                  limit=args.limit,
                  use_fts=not args.no_fts)

    print("\n== Resultados ==")
    for origen, codigo, snippet in rows:
        print(f"[{(origen or '-'):16s}] {(codigo or '-'):6s} :: {(snippet or '')}")
    if not rows:
        print("(sin resultados)")

    if args.export_csv:
        with open(args.export_csv, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["origen","cie10_code","snippet"])
            w.writerows(rows)
        print(f"[OK] Exportadas {len(rows)} filas a {args.export_csv}")

    conn.close()



if __name__ == "__main__":
    main()
