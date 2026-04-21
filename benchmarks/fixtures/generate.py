#!/usr/bin/env python3
"""Build benchmarks/fixtures/{corpus,queries}.jsonl.

Pulls ~10k short docs from a Hugging Face dataset, embeds each with
sentence-transformers/all-MiniLM-L6-v2 (384 dims, cosine-normal),
and writes one JSON object per line. Also picks ~200 queries and
records ground-truth doc ids via brute-force cosine similarity on
the embeddings themselves.

Outputs land in the same directory as this script.
"""

from __future__ import annotations

import argparse
import json
import os
import random
from pathlib import Path

import numpy as np
from datasets import load_dataset
from sentence_transformers import SentenceTransformer

HERE = Path(__file__).resolve().parent


def build_corpus(n_docs: int, seed: int) -> list[dict]:
    ds = load_dataset("ccdv/arxiv-classification", split="train", streaming=True)
    rng = random.Random(seed)
    seen_texts: set[str] = set()
    docs: list[dict] = []
    for row in ds:
        text = (row.get("text") or "").strip().replace("\n", " ")
        if not text or text in seen_texts:
            continue
        # Keep abstracts-length: trim to ~2 KB to keep fixture size reasonable.
        text = text[:2048]
        seen_texts.add(text)
        docs.append({"id": f"doc-{len(docs):05d}", "text": text})
        if len(docs) >= n_docs:
            break
    rng.shuffle(docs)
    return docs


def embed(docs: list[dict], model_name: str) -> np.ndarray:
    model = SentenceTransformer(model_name)
    texts = [d["text"] for d in docs]
    vecs = model.encode(texts, batch_size=64, show_progress_bar=True,
                        normalize_embeddings=True).astype(np.float32)
    return vecs


def pick_queries(docs: list[dict], vecs: np.ndarray, n_queries: int, k: int,
                 seed: int) -> list[dict]:
    """Use ~first sentence of randomly-picked docs as a query, record top-k
    via brute-force cosine as ground truth. vecs is already L2-normalised so
    cosine == dot."""
    rng = random.Random(seed)
    idx_pool = list(range(len(docs)))
    rng.shuffle(idx_pool)
    queries: list[dict] = []
    for i in idx_pool[:n_queries]:
        text = docs[i]["text"].split(".")[0][:256]
        if not text:
            continue
        q_vec = vecs[i]
        sims = vecs @ q_vec
        top = np.argpartition(-sims, k)[:k]
        top = top[np.argsort(-sims[top])]
        ground_truth = [docs[int(j)]["id"] for j in top]
        queries.append({
            "id": f"q-{len(queries):04d}",
            "text": text,
            "ground_truth": ground_truth,
        })
        if len(queries) >= n_queries:
            break
    return queries


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--n-docs", type=int, default=10_000)
    p.add_argument("--n-queries", type=int, default=200)
    p.add_argument("--k", type=int, default=10)
    p.add_argument("--model", default="sentence-transformers/all-MiniLM-L6-v2")
    p.add_argument("--seed", type=int, default=20260420)
    args = p.parse_args()

    docs = build_corpus(args.n_docs, args.seed)
    vecs = embed(docs, args.model)
    for d, v in zip(docs, vecs):
        d["embedding"] = v.tolist()
    queries = pick_queries(docs, vecs, args.n_queries, args.k, args.seed)

    corpus_path = HERE / "corpus.jsonl"
    queries_path = HERE / "queries.jsonl"
    with corpus_path.open("w") as f:
        for d in docs:
            f.write(json.dumps(d) + "\n")
    with queries_path.open("w") as f:
        for q in queries:
            f.write(json.dumps(q) + "\n")

    print(f"wrote {len(docs)} docs → {corpus_path}")
    print(f"wrote {len(queries)} queries → {queries_path}")


if __name__ == "__main__":
    main()
