# scry3

A **stock-Kythe** wrapper for AOSP-scale code walks. Where its sibling
[scry2](https://github.com/fiveapplesonthetable/scry2) replaces Kythe's
entire serving half with a custom `.s2db` packed-array engine, scry3 takes
the opposite bet: **keep everything stock.** The serving table is built by
Kythe's `write_tables`; every cross-reference / edge / node query is answered
by Kythe's own serving code. No custom storage engine, no custom query
engine to maintain.

scry3 adds only what stock Kythe is missing for a fast command line:

1. **Indexing orchestration** (`scry3 index`) — kzip → per-CU `Entry`
   streams, forked from scry2's battle-tested per-CU dispatch (sub-kzip
   extraction, AFDO-flag strip, `--inject-cu-arg`, crash isolation,
   parallelism). The **patched** indexers are reused unchanged.
2. **A name → ticket index** (`scry3 name-index`) — the one capability the
   *open-source* Kythe serving table lacks (`IdentifierMatch` is read but
   never written by the OSS `write_tables`). So you query by human name —
   `def android::Parcel::writeStrongBinder` — exactly like scry2; the ticket
   plumbing is invisible.
3. **A warm-server fast path** (`scry3 serve` / `scry3 repl`) — because the
   only thing slow about stock Kythe is per-query Go process startup (see
   benchmarks). Hold one process warm and queries drop to ~2 ms.

## Two ways to use it: expensive CLI, or warm server

```
# ── one-shot CLI (no setup, ~170 ms/query: spawns kythe each call) ──
scry3 --serving SERVING def     android::Parcel::writeStrongBinder
scry3 --serving SERVING ref     android::Parcel::writeInt32
scry3 --serving SERVING callers android::Parcel::writeInt32

# ── warm & fast (~2-3 ms/query, full Kythe data) ──
scry3 --serving SERVING repl                 # self-contained: auto-starts a
                                             # backend for the session
# or run a shared backend explicitly:
scry3 --serving SERVING serve --listen localhost:8089 &
scry3 --serving SERVING --http localhost:8089 def android::Parcel::writeStrongBinder
```

## Verbs (mirrors scry2)

```
# build pipeline
scry3 index        --kzip K --out-entries DIR [--in path,...] [--langs ...] [--resume]
scry3 build        --entries DIR --out SERVING [--mode sorted|graphstore|beam]
scry3 name-index   --entries DIR --out names.idx
# …or, for AOSP scale, one bounded-disk pass (index → GraphStore → serving + names):
scry3 index-stream --kzip K --out SERVING [--in path,...] [--langs ...]

# query  (NAME resolves via the name index; a kythe:// ticket passes through)
scry3 def NAME [--substr] [--in S] [--not-in S]    # definition site(s)
scry3 ref NAME            …                         # every reference
scry3 callers NAME        …                         # call sites
scry3 super NAME / sub NAME   [--in S] [--not-in S]  # supertypes / subtypes
scry3 callgraph NAME --direction up|down|both --depth N   # transitive call walk
scry3 identifier NAME / names PREFIX                 # name → ticket
scry3 edges NAME|TICKET / nodes … / decor PATH / ls # raw graph passthrough
scry3 stat                                           # serving + index health
scry3 repl / serve                                   # warm fast path
```

`--kythe-root` (or `$KYTHE_ROOT`) points at the Kythe release with the
**patched** `java_indexer.jar` / `jvm_indexer.jar` (`kythe-patches/`).
`--name-index` defaults to `<serving>/scry3.names.idx`. `--json` returns the
raw kythe reply.

## Benchmarks (one C++ CU, 3.06 M entries — full table in [docs/COMPARISON.md](docs/COMPARISON.md))

| | scry2 (`.s2db`) | scry3 (stock Kythe) |
|---|---|---|
| build: entries → queryable | **8 s** | ~133 s (`write_tables` + name-index) |
| artifact on disk | **47 MB** | 459 MB serving + 16 MB names |
| query, one-shot CLI | 40 ms | 170 ms (expensive path) |
| query, warm | **44 µs** (repl, in-proc mmap) | 1.75 ms (`--http repl`) / 3 ms (self-contained) |
| server-side query alone | ~1.8 µs | 537 µs |
| data per hit | byte offsets, curated | **snippet + span + kind, full graph** |

**Indexing (kzip → entries) is identical and stock for both** — it's Clang/
javac compiling each TU, the multi-hour AOSP cost; neither tool changes it.

## Should I bother with scry2?

* **Indexing:** no — identical to scry3.
* **Querying:** scry2's in-process mmap is the only thing that touches µs and
  high QPS, but it returns offsets only. scry3 with a warm server is ~2 ms
  with *full* Kythe data and **zero custom engine** — the better default for
  walking AOSP (LLM or human). Keep scry2 for the sub-ms / very-high-QPS
  niche. Full reasoning + "where else to shave overhead" in
  [docs/COMPARISON.md](docs/COMPARISON.md).

## Scale (AOSP)

The plain `index`+`build` path writes all entries to disk first — multiple TB
at AOSP scale. Use **`index-stream`** instead: it streams each CU straight
into a deduping GraphStore and deletes the per-CU file, so peak disk is the
GraphStore + serving table and **peak RAM is single-digit GB** (measured 2 GB
on the smoke, vs scry2's ~103 GB). See [docs/SCALING.md](docs/SCALING.md).

## Docs

* [docs/SCALING.md](docs/SCALING.md) — disk/RAM at AOSP scale and the `index-stream` bounded-disk path.
* [docs/PIPELINE.md](docs/PIPELINE.md) — kzip → serving table; why **non-beam** `write_tables`, and `graphstore` for all of AOSP.
* [docs/COMPARISON.md](docs/COMPARISON.md) — full benchmarks, why-slow analysis, and the scry2 verdict.
* [docs/AOSP.md](docs/AOSP.md) — end-to-end on a real AOSP slice.
* [kythe-patches/](kythe-patches/) — the four indexer patches required for AOSP Java/JVM cross-CU.

## Status

Validated end-to-end on a C++ slice (`frameworks/native/libs/binder/Parcel.cpp`):
`index` → `build` → `name-index` → `def`/`ref`/`callers`/`edges`/`nodes`/`repl`
all return correct, snippet-rich results. The warm fast path
(`serve`/`repl`) reaches ~2 ms/query.
