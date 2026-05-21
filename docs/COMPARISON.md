# scry3 vs scry2 — benchmarks, and "should I bother with scry2?"

Same input, same machine, head-to-head. Input: **one C++ compilation unit**
— `frameworks/native/libs/binder/Parcel.cpp` with all 1015 of its headers —
producing **3.06 M Kythe entries** (839 MB). Both tools start from the
*identical* `<sha>.entries` (the indexing stage is shared and stock).

Hardware: sandbox host (Xeon Gold 6148, 72 vCPU, 157 GB RAM, SSD).

## 1. Indexing (kzip → entries) — identical, and here's why it's slow

`scry3 index` ran in ~14 s for this one CU. That time is **`cxx_indexer`
running a full Clang parse of Parcel.cpp + 1015 headers** to build the
semantic graph — it is literally compiling the translation unit. Java CUs
similarly run `javac`. This is inherent to Kythe and **identical for scry2
and scry3** (same indexer binaries, same patches). It is the multi-hour part
of an AOSP build, but it is embarrassingly parallel per-CU (our run used 36
workers), so wall time = total compile work / workers.

**Takeaway:** indexing speed is not a scry2-vs-scry3 question. Neither tool
can make it faster; both just orchestrate the same indexers.

## 2. Build (entries → queryable store)

| | scry2 (`.s2db`) | scry3 (stock Kythe) |
|---|---|---|
| build wall | **8.4 s** | 122 s (`write_tables`) + 11 s (name-index) = **~133 s** |
| artifact | **47 MB** | 459 MB serving table + 16 MB name index |

scry2 builds **~16× faster** and is **~10× smaller**. The gap is structural:
scry2 sorts 6 fixed-width arrays in RAM and flushes once; `write_tables`
external-sorts and denormalizes the graph into general serving tables
(per-file decorations, paginated xrefs/edges, snippets) — the data that
powers the rich queries.

## 3. Query latency

| path | scry2 | scry3 |
|---|---|---|
| **server-side query** (warm, no transport) | ~1.8 µs (mmap) | **537 µs** (`http_server` access log) |
| one-shot CLI (cold process) | 40 ms | 170 ms (the "expensive" path) |
| warm / amortized | **44 µs** (`scry2 repl`, in-proc mmap) | **1.75 ms** (`--http repl`), 3.1 ms (self-contained `repl`) |

### Why scry3 one-shot is slow (and it's *not* the file format)

`strace` of one warm `kythe xrefs`: **89 % of the time is `futex` — Go
runtime/GC startup.** mmap 3.5 ms, file reads <1 ms. So the ~40 ms one-shot
floor is **Go process startup**, not LevelDB, not the serving format, not
roundtrips. Compaction / a different DB would not move it.

### How scry3 gets fast: keep the Go process warm

`scry3 serve` runs one `http_server` that holds LevelDB open; queries hit it
over a kept-alive socket from scry3's own in-process HTTP client — no
per-query process spawn. The server's own query is **537 µs**; the rest of
the **1.75 ms** is the localhost HTTP round-trip + JSON encode/parse. The
self-contained `scry3 repl` auto-starts that backend, so you get ~3 ms with
zero setup.

### Why scry2 is still faster warm (44 µs vs 1.75 ms)

scry2 is an mmap in the *same* process: a query is a `memcmp` binary search
over packed bytes — no socket, no JSON, no process boundary. scry3's 1.75 ms
is the price of reusing Kythe's serving stack across a process boundary. The
~40× gap is entirely transport + serialization, not the lookup itself.

## 4. What you get per hit (the other side of the ledger)

* **scry2**: byte offsets (`Parcel.cpp@58156`) and a curated edge subset.
* **scry3**: source **snippet**, full line/column **span**, every **edge
  kind**, node **facts** (`/kythe/node/kind`, `/kythe/complete`, MarkedSource,
  visibility), file **decorations**, related nodes — straight from stock
  Kythe, nothing re-derived.

```
$ scry3 --http localhost:8089 ref android::Parcel::writeInt32
  frameworks/native/libs/binder/Parcel.cpp:228:11  return writeInt32(rep);
  frameworks/native/libs/binder/Parcel.cpp:1127:8  writeInt32(threadState->getStrictModePolicy() | ...);
```

## 5. So — should I bother with scry2?

**Indexing:** No. Identical to scry3; nothing to gain.

**Building the store:** scry2 wins on build time (16×) and disk (10×). If you
rebuild constantly or are tight on disk, that matters.

**Querying — it depends on your access pattern:**

| you do… | use |
|---|---|
| thousands of lookups/sec, µs latency, offsets are enough | **scry2** (in-proc mmap; nothing else touches µs) |
| hundreds of queries, want snippets / edges / decorations | **scry3** self-contained `repl` (~3 ms, full data) |
| many client processes sharing one warm index | **scry3 serve** + `--http` (~1.75 ms, full data) |

**The middle ground (best default for code-walking, incl. LLMs):**
**scry3 with a warm server.** ~2 ms/query, *full* Kythe fidelity, and **zero
custom storage/query engine to maintain** — the serving table and every
query are stock Kythe. scry2's value is a genuine but narrow niche:
sub-millisecond, very-high-QPS lookups where the lean schema suffices. For
most "walk the AOSP graph and show me code" work, scry3 is the better trade.

## 6. Where else could scry3 shave overhead?

1. **Name-index reload (~60 ms, one-shot only).** Each one-shot reloads the
   text name index. Borrow scry2's mmap-fixed-width-rows trick → µs, no load.
   `repl`/`serve` already amortize this to zero.
2. **JSON encode/parse (part of the 1.75 ms).** Switch the warm transport to
   the server's protobuf/gRPC endpoint instead of JSON HTTP. Modest win.
3. **LevelDB compaction.** Real but small — the open cost is dwarfed by Go
   startup; only helps the one-shot path, which is "expensive" by design.
4. **The 40 ms Go-startup floor** is only payable on the one-shot path and is
   unavoidable without a warm process — which is exactly what `serve` is.

Reproduce: see [`PIPELINE.md`](PIPELINE.md). Numbers above are warm medians of
5–200 runs.
