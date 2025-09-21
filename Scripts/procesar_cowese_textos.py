#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Procesamiento de textos (CoWeSe) para la BD heterogénea federada
- Lee CoWeSe.txt (o cualquier texto grande en español)
- Divide en oraciones (split básico)
- Detecta menciones relacionadas con consumo de sustancias (F10..F19) via diccionario de palabras clave
- Exporta matches a CSV (cowese_matches.csv)
- Construye un índice TF-IDF de oraciones (cowese_tfidf.pkl, cowese_vectorizer.pkl) para búsquedas rápidas
Uso:
    python procesar_cowese_textos.py --cowese /ruta/CoWeSe.txt --outdir ./Textos
    # Consulta posterior del índice:
    python procesar_cowese_textos.py --query "intoxicación por alcohol en jóvenes"
Requisitos:
    pip install pandas scikit-learn
"""
import argparse
import csv
import re
from pathlib import Path
from typing import List, Dict, Tuple, Iterable, Optional
import pandas as pd
import pickle

# ============ Palabras clave -> CIE-10 ============
KEYS_TO_CIE10 = {
    "alcohol": "F10",
    "etílico": "F10",
    "etilico": "F10",
    "bebidas alcohólicas": "F10",
    "bebida alcohólica": "F10",
    "alcohólico": "F10",
    "alcoholico": "F10",
    "cocaína": "F14",
    "cocaina": "F14",
    "crack": "F14",
    "opioides": "F11",
    "morfina": "F11",
    "heroína": "F11",
    "heroina": "F11",
    "fentanilo": "F11",
    "cannabis": "F12",
    "marihuana": "F12",
    "thc": "F12",
    "sedantes": "F13",
    "benzodiacepinas": "F13",
    "clonazepam": "F13",
    "diazepam": "F13",
    "hipnóticos": "F13",
    "hipnoticos": "F13",
    "anfetamina": "F15",
    "anfetaminas": "F15",
    "metanfetamina": "F15",
    "metanfetaminas": "F15",
    "mdma": "F15",
    "éxtasis": "F15",
    "extasis": "F15",
    "tabaco": "F17",
    "nicotina": "F17",
    "cigarrillo": "F17",
    "solventes": "F18",
    "inhalables": "F18",
    "thinner": "F18",
    "alucinógenos": "F16",
    "alucinogenos": "F16",
    "lsd": "F16",
    "psilocibina": "F16",
    "peyote": "F16",
    "mezcla de sustancias": "F19",
    "poli consumo": "F19",
    "policonsumo": "F19",
    "múltiples sustancias": "F19",
    "multiples sustancias": "F19",
}

# ============ Utilidades de texto ============
SENT_SPLIT_RE = re.compile(r'(?<=[\.\!\?;:])\s+(?=[A-ZÁÉÍÓÚÑ])')

def iter_sentences(stream: Iterable[str]) -> Iterable[str]:
    buf = ""
    for line in stream:
        line = line.strip()
        if not line:
            continue
        if buf:
            buf += " " + line
        else:
            buf = line
        parts = SENT_SPLIT_RE.split(buf)
        for s in parts[:-1]:
            s = s.strip()
            if s:
                yield s
        buf = parts[-1] if parts else ""
    if buf.strip():
        yield buf.strip()

def find_mentions(sentence: str, keys_to_cie10: Dict[str,str]) -> List[Tuple[str,str]]:
    low = sentence.lower()
    hits = []
    for k, code in keys_to_cie10.items():
        if k in low:
            hits.append((k, code))
    seen = set()
    result = []
    for k, code in hits:
        if code not in seen:
            result.append((k, code))
            seen.add(code)
    return result

# ============ TF-IDF Index ============
def build_tfidf_index(sentences: List[str], outdir: Path) -> None:
    from sklearn.feature_extraction.text import TfidfVectorizer
    spanish_sw = [
        "de","la","que","el","en","y","a","los","del","se","las","por","un","para","con",
        "no","una","su","al","lo","como","más","mas","pero","sus","le","ya","o","este","sí","si","porque","esta",
        "entre","cuando","muy","sin","sobre","también","tambien","me","hasta","hay","donde","quien"
    ]
    vectorizer = TfidfVectorizer(
        lowercase=True,
        stop_words=spanish_sw,
        max_df=0.8,
        min_df=2,
        ngram_range=(1,2)
    )
    X = vectorizer.fit_transform(sentences)
    with open(outdir / "cowese_vectorizer.pkl", "wb") as f:
        pickle.dump(vectorizer, f)
    with open(outdir / "cowese_tfidf.pkl", "wb") as f:
        pickle.dump(X, f)
    print(f"[OK] Índice TF-IDF creado. {X.shape[0]} oraciones, vocab {len(vectorizer.vocabulary_)}")

def tfidf_search(query: str, outdir: Path, topk: int = 10) -> pd.DataFrame:
    with open(outdir / "cowese_vectorizer.pkl", "rb") as f:
        vectorizer = pickle.load(f)
    with open(outdir / "cowese_tfidf.pkl", "rb") as f:
        X = pickle.load(f)
    qv = vectorizer.transform([query])
    import numpy as np
    sims = (X @ qv.T).toarray().ravel()
    top_idx = np.argsort(-sims)[:topk]
    sentences_csv = outdir / "cowese_sentences.csv"
    if sentences_csv.exists():
        df_sent = pd.read_csv(sentences_csv)
        subset = df_sent.iloc[top_idx][["doc_id","sent_id","sentence"]].copy()
    else:
        subset = pd.DataFrame({"idx": top_idx, "score": sims[top_idx]})
    subset["score"] = sims[top_idx]
    return subset.reset_index(drop=True)

# ============ Pipeline principal ============
def process_cowese(cowese_path: Path, outdir: Path, limit_docs: Optional[int] = None):
    outdir.mkdir(parents=True, exist_ok=True)
    sentences = []
    with cowese_path.open("r", encoding="utf-8", errors="ignore") as f:
        doc_id = 0
        for sentence in iter_sentences(f):
            sentences.append((doc_id, len(sentences), sentence))
            if limit_docs and len(sentences) >= limit_docs:
                break
        print(f"[OK] Se extrajeron {len(sentences)} oraciones")
    df_sent = pd.DataFrame(sentences, columns=["doc_id","sent_id","sentence"])
    df_sent.to_csv(outdir / "cowese_sentences.csv", index=False)

    rows = []
    for _, row in df_sent.iterrows():
        s = row["sentence"]
        hits = find_mentions(s, KEYS_TO_CIE10)
        for k, code in hits:
            rows.append({
                "doc_id": row["doc_id"],
                "sent_id": row["sent_id"],
                "sentence": s,
                "keyword": k,
                "cie10": code
            })
    df_matches = pd.DataFrame(rows, columns=["doc_id","sent_id","sentence","keyword","cie10"])
    df_matches.to_csv(outdir / "cowese_matches.csv", index=False, quoting=csv.QUOTE_MINIMAL)
    print(f"[OK] Matches guardados: {len(df_matches)} filas en {outdir/'cowese_matches.csv'}")

    build_tfidf_index(df_sent["sentence"].tolist(), outdir)

def main():
    ap = argparse.ArgumentParser(description="Procesar CoWeSe.txt para BD heterogénea.")
    ap.add_argument("--cowese", type=str, help="Ruta a CoWeSe.txt")
    ap.add_argument("--outdir", type=str, default="./Textos")
    ap.add_argument("--limit", type=int, default=None, help="Limitar número de oraciones")
    ap.add_argument("--query", type=str, default=None, help="Consulta sobre el índice TF-IDF ya creado")
    args = ap.parse_args()

    outdir = Path(args.outdir).resolve()
    if args.query:
        res = tfidf_search(args.query, outdir=outdir, topk=10)
        print(res.head(10).to_string(index=False))
        return

    if not args.cowese:
        raise SystemExit("Debes indicar --cowese /ruta/CoWeSe.txt o usar --query con un índice ya creado")

    cowese_path = Path(args.cowese).resolve()
    if not cowese_path.exists():
        raise SystemExit(f"No existe el archivo: {cowese_path}")

    process_cowese(cowese_path, outdir, limit_docs=args.limit)

if __name__ == "__main__":
    main()
