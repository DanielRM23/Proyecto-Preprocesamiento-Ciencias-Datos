#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
build_base_final.py
Construye la base federada (SQLite) automáticamente:
- Si existen archivos LIMPIOS (dedup): carga esos
- Si no, busca archivos CRUDOS (F10..F19 anchos + cowese_matches) y los procesa

Uso:
    python Scripts/build_base_final.py
    python Scripts/build_base_final.py --outdb salud_federada.db
"""

import os, sys, re, sqlite3, argparse
import pandas as pd
from pathlib import Path

# ------------------ Config (rutas por defecto) ------------------
ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "Data"
TEXT = DATA / "Textos"
DB_DEFAULT = ROOT / "salud_federada.db"

# LIMPIOS
DEF_CLEAN   = DATA / "Limpieza/defunciones_uso_sustancias_dedup.csv"
URG_CLEAN   = DATA / "Limpieza/urgencias_uso_sustancias_dedup.csv"
NODES_CLEAN = DATA / "Limpieza/cie10_nodes_dedup.csv"
EDGES_CLEAN = DATA / "Limpieza/cie10_edges_dedup.csv"
TXT_PHRASES = TEXT / "textos_cie10_frases.csv"
TXT_MAP     = TEXT / "textos_cie10_frases_x_docs.csv"

# CRUDOS
DEF_RAW   = DATA / "defunciones_uso_sustancias_clean.csv"
URG_RAW   = DATA / "urgencias_uso_sustancias_clean.csv"
NODES_RAW = DATA / "cie10_f10_f19_nodes.csv"
EDGES_RAW = DATA / "cie10_f10_f19_edges_enriched.csv"
TXT_RAW   = TEXT / "cowese_matches.csv"

F_CODES = [f"F{n}" for n in range(10,20)]  # F10..F19
CIE_PATTERN = re.compile(r"^F1[0-9](\..+)?$", re.I)

# ------------------ Utilidades ------------------
def load_csv_safe(path: Path):
    if not path.exists():
        print(f"[WARN] No existe: {path}", file=sys.stderr)
        return None
    for enc in ("utf-8","latin-1"):
        try:
            return pd.read_csv(path, sep=None, engine="python", encoding=enc)
        except Exception:
            continue
    return pd.read_csv(path, encoding="latin-1")

def connect_db(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(str(path))
    con.execute("PRAGMA foreign_keys = ON;")
    con.execute("PRAGMA journal_mode = WAL;")
    con.execute("PRAGMA synchronous = NORMAL;")
    return con

def detect_year_col(df, candidates=("anio","anio_defuncion","year","fecha","periodo")):
    for c in candidates:
        if c in df.columns:
            return c
    for col in df.columns:
        cl = col.lower()
        if "anio" in cl or "año" in cl or "year" in cl:
            return col
    return None

def normalize_entidad_col(df):
    df = df.copy()
    # entidad_norm
    for col in ["entidad_norm","entidad_defuncion_etq","entidad","entidad_defuncion","estado","Entidad","nom_ent","nombre_entidad"]:
        if col in df.columns:
            df["entidad_norm"] = df[col]
            break
    if "entidad_norm" not in df.columns:
        df["entidad_norm"] = pd.NA
    # (Se elimina cualquier manejo de cve_entidad)
    return df

def wide_to_tidy(df, fuente):
    """De ancho (F10..F19) a tidy estándar (sin cve_entidad)."""
    df = df.copy()
    ycol = detect_year_col(df)
    if ycol:
        try:
            dt = pd.to_datetime(df[ycol], errors="coerce")
            if dt.notna().any():
                df["anio"] = dt.dt.year
            else:
                df["anio"] = pd.to_numeric(df[ycol], errors="coerce")
                if df["anio"].isna().all():
                    df = df.rename(columns={ycol:"anio"})
        except Exception:
            df = df.rename(columns={ycol:"anio"})
    else:
        df["anio"] = pd.NA

    df = normalize_entidad_col(df)
    if "edad_quinquenal" not in df.columns:
        df["edad_quinquenal"] = pd.NA
    if "sexo" not in df.columns:
        df["sexo"] = pd.NA

    f_present = [c for c in F_CODES if c in df.columns]
    if not f_present:
        raise ValueError("No se detectaron columnas F10..F19 en formato ancho.")

    keep = ["anio","entidad_norm","edad_quinquenal","sexo"]
    melted = df[keep + f_present].melt(
        id_vars=keep, value_vars=f_present,
        var_name="cie10_code", value_name="valor"
    )
    melted["cie10_code"] = melted["cie10_code"].astype(str).str.upper().str.strip()
    melted["valor"] = pd.to_numeric(melted["valor"], errors="coerce").fillna(0).astype(int)
    melted["fuente"] = fuente
    return melted

# ------------------ Schema/Vistas/Índices ------------------
def create_schema(con, use_text_catalog=True):
    cur = con.cursor()

    # Suspende FKs mientras reseteamos el esquema
    cur.execute("PRAGMA foreign_keys = OFF;")

    # Primero: eliminar vistas que puedan referenciar tablas
    cur.executescript("""
    DROP VIEW IF EXISTS v_unificado;
    DROP VIEW IF EXISTS v_eventos;
    DROP VIEW IF EXISTS v_eventos_por_codigo;
    DROP VIEW IF EXISTS v_eventos_con_texto;
    DROP VIEW IF EXISTS v_relaciones_epidemiologicas;
    DROP VIEW IF EXISTS v_texto_por_codigo;
    """)

    # Luego: dropear tablas HIJAS antes que PADRES
    if use_text_catalog:
        cur.executescript("""
        DROP TABLE IF EXISTS texto_frases_x_docs;   -- hija
        DROP TABLE IF EXISTS texto_frases;          -- padre
        """)
    else:
        cur.executescript("DROP TABLE IF EXISTS texto_matches;")

    cur.executescript("""
    DROP TABLE IF EXISTS cie10_edges;   -- hija
    DROP TABLE IF EXISTS cie10_nodes;   -- padre

    DROP TABLE IF EXISTS fact_defunciones;
    DROP TABLE IF EXISTS fact_urgencias;
    """)

    # Crear esquema limpio
    cur.executescript("""
    CREATE TABLE fact_defunciones (
      anio INTEGER,
      entidad_norm TEXT,
      edad_quinquenal TEXT,
      sexo TEXT,
      cie10_code TEXT,
      valor INTEGER,
      fuente TEXT DEFAULT 'defunciones'
    );
    CREATE TABLE fact_urgencias (
      anio INTEGER,
      entidad_norm TEXT,
      edad_quinquenal TEXT,
      sexo TEXT,
      cie10_code TEXT,
      valor INTEGER,
      fuente TEXT DEFAULT 'urgencias'
    );

    CREATE TABLE cie10_nodes(
      code TEXT PRIMARY KEY,
      descripcion TEXT
    );
    CREATE TABLE cie10_edges(
      source TEXT,
      target TEXT,
      rel_type TEXT,
      FOREIGN KEY(source) REFERENCES cie10_nodes(code),
      FOREIGN KEY(target) REFERENCES cie10_nodes(code)
    );
    """)

    if use_text_catalog:
        cur.executescript("""
        CREATE TABLE texto_frases (
          phrase_hash TEXT PRIMARY KEY,
          cie10_code  TEXT,
          sentence_raw   TEXT,
          sentence_norm  TEXT,
          n_ocurrencias  INTEGER
        );
        CREATE TABLE texto_frases_x_docs (
          phrase_hash TEXT,
          doc_id INTEGER,
          sent_id INTEGER,
          PRIMARY KEY (phrase_hash, doc_id, sent_id),
          FOREIGN KEY (phrase_hash) REFERENCES texto_frases(phrase_hash)
        );
        """)

    # Reactivar FKs
    cur.execute("PRAGMA foreign_keys = ON;")
    con.commit()


def create_views(con, use_text_catalog=True):
    cur = con.cursor()
    cur.executescript("""
    DROP VIEW IF EXISTS v_eventos;
    CREATE VIEW v_eventos AS
    SELECT anio, entidad_norm, edad_quinquenal, sexo, cie10_code, valor, 'defunciones' AS fuente
      FROM fact_defunciones
    UNION ALL
    SELECT anio, entidad_norm, edad_quinquenal, sexo, cie10_code, valor, 'urgencias'   AS fuente
      FROM fact_urgencias;

    DROP VIEW IF EXISTS v_relaciones_epidemiologicas;
    CREATE VIEW v_relaciones_epidemiologicas AS
    SELECT source AS cie10_a, target AS cie10_b, rel_type AS tipo
    FROM cie10_edges
    WHERE rel_type IS NOT NULL;

    DROP VIEW IF EXISTS v_eventos_por_codigo;
    CREATE VIEW v_eventos_por_codigo AS
    SELECT cie10_code, fuente, SUM(valor) total
    FROM v_eventos
    GROUP BY cie10_code, fuente;
    """)
    if use_text_catalog:
        cur.executescript("""
        DROP VIEW IF EXISTS v_texto_por_codigo;
        CREATE VIEW v_texto_por_codigo AS
        SELECT cie10_code, sentence_norm, n_ocurrencias
        FROM texto_frases;
        """)
    else:
        cur.executescript("""
        DROP VIEW IF EXISTS v_eventos_con_texto;
        CREATE VIEW v_eventos_con_texto AS
        SELECT e.anio, e.entidad_norm, e.cie10_code, e.valor, e.fuente,
               t.doc_id, t.sent_id, t.keyword, t.sentence
        FROM v_eventos e
        LEFT JOIN texto_matches t USING (cie10_code);
        """)
    con.commit()

def create_indexes(con, use_text_catalog=True):
    cur = con.cursor()
    cur.executescript("""
    CREATE INDEX IF NOT EXISTS idx_def_keys ON fact_defunciones(anio, entidad_norm, cie10_code);
    CREATE INDEX IF NOT EXISTS idx_urg_keys ON fact_urgencias(anio, entidad_norm, cie10_code);
    CREATE INDEX IF NOT EXISTS idx_nodes_code ON cie10_nodes(code);
    CREATE INDEX IF NOT EXISTS idx_edges_src  ON cie10_edges(source);
    CREATE INDEX IF NOT EXISTS idx_edges_tgt  ON cie10_edges(target);
    CREATE INDEX IF NOT EXISTS idx_edges_rel  ON cie10_edges(rel_type);
    """)
    if use_text_catalog:
        cur.executescript("""
        CREATE INDEX IF NOT EXISTS idx_txt_code ON texto_frases(cie10_code);
        CREATE INDEX IF NOT EXISTS idx_map_hash ON texto_frases_x_docs(phrase_hash);
        """)
        # FTS opcional (si está disponible)
        try:
            cur.executescript("""
            DROP TABLE IF EXISTS texto_frases_fts;
            CREATE VIRTUAL TABLE texto_frases_fts
            USING fts5(sentence_norm, content='texto_frases', content_rowid='rowid');
            INSERT INTO texto_frases_fts(rowid, sentence_norm)
            SELECT rowid, sentence_norm FROM texto_frases;
            """)
        except Exception as e:
            print("[WARN] FTS5 no disponible:", e)
    else:
        cur.executescript("CREATE INDEX IF NOT EXISTS idx_texto_cie10 ON texto_matches(cie10_code);")
    con.commit()

# ------------------ Carga LIMPIOS / CRUDOS ------------------
def build_from_clean(con):
    print("[INFO] Construyendo desde LIMPIOS (dedup)…")
    create_schema(con, use_text_catalog=True)

    # Hechos
    dfd = load_csv_safe(DEF_CLEAN)
    dfu = load_csv_safe(URG_CLEAN)
    for df, name in [(dfd,"defunciones"), (dfu,"urgencias")]:
        if df is None:
            print(f"[ERROR] Falta CSV limpio: {name}", file=sys.stderr); sys.exit(1)
        need = {"anio","entidad_norm","sexo","edad_quinquenal","cie10_code","valor"}
        miss = need - set(df.columns)
        if miss:
            print(f"[ERROR] {name} dedup faltan columnas: {miss}", file=sys.stderr); sys.exit(1)
    dfd.to_sql("fact_defunciones", con, if_exists="append", index=False)
    dfu.to_sql("fact_urgencias",  con, if_exists="append", index=False)

    # Grafo
    nodes = load_csv_safe(NODES_CLEAN)
    edges = load_csv_safe(EDGES_CLEAN)
    if nodes is None or edges is None:
        print("[ERROR] Faltan nodes/edges limpios", file=sys.stderr); sys.exit(1)
    if "code" not in nodes.columns:
        if "cie10_code" in nodes.columns:
            nodes = nodes.rename(columns={"cie10_code":"code"})
        else:
            print("[ERROR] nodes_dedup sin columna 'code'", file=sys.stderr); sys.exit(1)
    keep_nodes = ["code"] + ([c for c in ["descripcion"] if c in nodes.columns])
    nodes[keep_nodes].drop_duplicates(subset=["code"]).to_sql("cie10_nodes", con, if_exists="append", index=False)

    for c in ["source","target","rel_type"]:
        if c not in edges.columns:
            print(f"[ERROR] edges_dedup falta columna: {c}", file=sys.stderr); sys.exit(1)
    keep_edges = ["source","target","rel_type"]
    edges[keep_edges].drop_duplicates().to_sql("cie10_edges", con, if_exists="append", index=False)

    # Textos (frases + mapeo)
    phrases = load_csv_safe(TXT_PHRASES)
    mapping = load_csv_safe(TXT_MAP)
    if phrases is None or mapping is None:
        print("[ERROR] Faltan frases/mapa limpios", file=sys.stderr); sys.exit(1)
    need_p = {"phrase_hash","cie10_code","sentence_norm"}
    miss_p = need_p - set(phrases.columns)
    need_m = {"phrase_hash","doc_id","sent_id"}
    miss_m = need_m - set(mapping.columns)
    if miss_p or miss_m:
        print(f"[ERROR] text_frases/map faltan columnas: {miss_p or ''} {miss_m or ''}", file=sys.stderr); sys.exit(1)
    phrases.to_sql("texto_frases", con, if_exists="append", index=False)
    mapping.to_sql("texto_frases_x_docs", con, if_exists="append", index=False)

    create_views(con, use_text_catalog=True)
    create_indexes(con, use_text_catalog=True)

def build_from_raw(con):
    print("[INFO] Construyendo desde CRUDOS…")
    create_schema(con, use_text_catalog=False)

    # Hechos (ancho → tidy)
    df_def = load_csv_safe(DEF_RAW)
    df_urg = load_csv_safe(URG_RAW)
    if df_def is None and df_urg is None:
        print("[ERROR] No hay CSV crudos (defunciones/urgencias).", file=sys.stderr); sys.exit(1)
    if df_def is not None:
        tidy_def = wide_to_tidy(df_def, "defunciones")
        tidy_def.to_sql("fact_defunciones", con, if_exists="append", index=False)
        print(f"[OK] Defunciones (crudo→tidy): {len(tidy_def):,}")
    if df_urg is not None:
        tidy_urg = wide_to_tidy(df_urg, "urgencias")
        tidy_urg.to_sql("fact_urgencias", con, if_exists="append", index=False)
        print(f"[OK] Urgencias (crudo→tidy): {len(tidy_urg):,}")

    # Grafo
    nodes = load_csv_safe(NODES_RAW)
    edges = load_csv_safe(EDGES_RAW)
    if nodes is not None:
        if "cie10_code" in nodes.columns and "code" not in nodes.columns:
            nodes = nodes.rename(columns={"cie10_code":"code"})
        if "descripcion" not in nodes.columns:
            for cand in ["desc","description","label","nombre","name"]:
                if cand in nodes.columns:
                    nodes = nodes.rename(columns={cand:"descripcion"})
                    break
        keep = [c for c in ["code","descripcion"] if c in nodes.columns]
        nodes[keep].to_sql("cie10_nodes", con, if_exists="append", index=False)
        print(f"[OK] Nodes (crudo): {len(nodes):,}")
    if edges is not None:
        ren = {}
        if "source" not in edges.columns:
            for c in ["src","from","origen"]:
                if c in edges.columns: ren[c]="source"; break
        if "target" not in edges.columns:
            for c in ["dst","to","destino"]:
                if c in edges.columns: ren[c]="target"; break
        if "rel_type" not in edges.columns and "tipo" in edges.columns:
            ren["tipo"] = "rel_type"
        elif "rel_type" not in edges.columns:
            for c in ["type","relation","label"]:
                if c in edges.columns: ren[c]="rel_type"; break
        if ren:
            edges = edges.rename(columns=ren)
        keep = [c for c in ["source","target","rel_type"] if c in edges.columns]
        edges[keep].to_sql("cie10_edges", con, if_exists="append", index=False)
        print(f"[OK] Edges (crudo): {len(edges):,}")

    # Texto crudo (cowese_matches)
    tm = load_csv_safe(TXT_RAW)
    if tm is not None:
        ren = {}
        for a,b in [("cie10","cie10_code"),("doc","doc_id"),("sent","sent_id")]:
            if a in tm.columns and b not in tm.columns:
                ren[a]=b
        tm = tm.rename(columns=ren)
        keep = [c for c in ["doc_id","sent_id","sentence","keyword","cie10_code","anio","cve_entidad"] if c in tm.columns]
        tm = tm[keep]
        tm.to_sql("texto_matches", con, if_exists="append", index=False)
        print(f"[OK] texto_matches (crudo): {len(tm):,}")

    create_views(con, use_text_catalog=False)
    create_indexes(con, use_text_catalog=False)

# ------------------ QA exprés ------------------
def qa_summary(con):
    cur = con.cursor()
    print("\n[RESUMEN v_eventos]")
    try:
        for src, cnt, tot in cur.execute("SELECT fuente, COUNT(*), SUM(valor) FROM v_eventos GROUP BY fuente;"):
            print(f"  {src:12s}  filas={cnt:,}  suma_valor={tot:,}")
    except Exception as e:
        print("  (vacio)", e)

    print("\n[QA] Duplicados clave (def/urg):")
    q_dup = """
    SELECT COUNT(*) FROM (
      SELECT anio, entidad_norm, sexo, edad_quinquenal, cie10_code, COUNT(*) c
      FROM {tbl}
      GROUP BY 1,2,3,4,5 HAVING c>1
    );
    """
    try:
        d1 = cur.execute(q_dup.format(tbl="fact_defunciones")).fetchone()[0]
        d2 = cur.execute(q_dup.format(tbl="fact_urgencias")).fetchone()[0]
        print(f"  defunciones={d1} | urgencias={d2}")
    except Exception as e:
        print("  (no disponible)", e)

    print("\n[QA] Códigos en hechos NO presentes en nodes:")
    try:
        missing = cur.execute("""
            SELECT COUNT(*) FROM (
              SELECT DISTINCT cie10_code FROM v_eventos
              EXCEPT
              SELECT code FROM cie10_nodes
            );
        """).fetchone()[0]
        print(f"  faltantes={missing}")
    except Exception as e:
        print("  (no disponible)", e)

# ------------------ main ------------------
def main():
    ap = argparse.ArgumentParser(description="Constructor automático de salud_federada.db (clean primero, si no raw).")
    ap.add_argument("--outdb", type=str, default=str(DB_DEFAULT), help="Ruta de salida de la base SQLite")
    args = ap.parse_args()

    # Autodetección
    have_clean = all(p.exists() for p in [DEF_CLEAN, URG_CLEAN, NODES_CLEAN, EDGES_CLEAN, TXT_PHRASES, TXT_MAP])
    have_raw   = any(p.exists() for p in [DEF_RAW, URG_RAW, NODES_RAW, EDGES_RAW, TXT_RAW])

    con = connect_db(Path(args.outdb))
    try:
        if have_clean:
            build_from_clean(con)
        elif have_raw:
            build_from_raw(con)
        else:
            print("[ERROR] No se encontraron insumos ni LIMPIOS ni CRUDOS.", file=sys.stderr)
            print("  Esperados LIMPIOS:", DEF_CLEAN, URG_CLEAN, NODES_CLEAN, EDGES_CLEAN, TXT_PHRASES, TXT_MAP, sep="\n  - ")
            print("  O bien CRUDOS:", DEF_RAW, URG_RAW, NODES_RAW, EDGES_RAW, TXT_RAW, sep="\n  - ")
            sys.exit(1)

        qa_summary(con)
        print(f"\n[OK] Base creada en: {Path(args.outdb).resolve()}")
    finally:
        con.close()

if __name__ == "__main__":
    main()
