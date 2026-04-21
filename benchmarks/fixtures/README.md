# Benchmark Fixtures

`queries.jsonl` is committed; `corpus.jsonl` is **not** — it is large
(~100 MB of embeddings) and must be regenerated locally before running
the benchmarks or the Python demo. `generate.py` produces both, and
both are deterministic from `--seed`, so a regenerated `corpus.jsonl`
matches the committed `queries.jsonl` when run with default flags.

## Generate (required before benches or demo)

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python3 generate.py
```

First run downloads `sentence-transformers/all-MiniLM-L6-v2` (~80 MB)
and takes a few minutes to embed 10k abstracts. Subsequent runs reuse
the Hugging Face cache.

## Flags

- `--n-docs` (default `10000`)
- `--n-queries` (default `200`)
- `--k` top-k for ground-truth (default `10`)
- `--model` any sentence-transformers model; the `embedding` dim must
  match AgentDB's configured dim (currently 384). If you pick a
  different-dim model, also change the Postgres schemas
  (`benchmarks/postgres/tuned.sql`) and AgentDB's index config.
- `--seed` RNG seed (default `20260420`). **Changing the seed makes
  the regenerated corpus's doc ids drift from the committed
  `queries.jsonl` — regenerate `queries.jsonl` too if you do this.**

## File format

Each line of `corpus.jsonl` (locally-generated, gitignored):

```json
{"id": "doc-00000", "text": "...", "embedding": [0.01, -0.02, ...]}
```

Each line of `queries.jsonl` (committed):

```json
{"id": "q-0000", "text": "...", "ground_truth": ["doc-01234", "doc-00567", ...]}
```

`ground_truth` is the top-10 doc ids by brute-force cosine similarity
over the embeddings at generation time. Used for recall@10 in Bench 4.
