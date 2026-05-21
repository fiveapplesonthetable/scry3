# Scaling scry3 to AOSP — disk, RAM, and the streaming path

The plain `index` + `build` path writes **every CU's entries to disk** before
building. Those entries are *not* deduplicated across CUs — each CU re-emits
all of its shared header symbols — so for an AOSP-scale corpus the entries
directory is enormous.

## The disk problem (measured)

One C++ CU, `Parcel.cpp` (a header-heavy TU), produced **864 MB** of entries.
The AOSP kzip has **118,647 CUs** (106,670 C++). Even at a conservative
average, the full entry set is multiple terabytes:

| avg entries / CU | full-AOSP entries on disk |
|---|---|
| 40 MB | ~4.7 TB |
| 80 MB | ~9.5 TB |
| 150 MB (Parcel-ish) | ~18 TB |

A typical box has hundreds of GB free, not tens of TB. **`index` → `build`
does not fit at AOSP scale.**

## How the other tools handle it

* **scry2** streams each CU into an in-RAM builder and never stores entries —
  trading disk for RAM. Measured on a live `frameworks/*`+`system`+`art`+
  `libcore` slice (23,597 CUs): **~103 GB RAM**.
* **scry3 `index`/`build`** trades RAM for disk — and loses, because the disk
  bill is multi-TB.

## The fix: `index-stream` (bounded disk *and* bounded RAM)

`scry3 index-stream` does index → GraphStore → serving table in one pass:

```
 N indexer workers ──(bounded channel)──▶ 1 folder
   sub-kzip → indexer → tmp .entries        batch → write_entries → GraphStore
   scan names (shared set)                   delete the batch
 ───────────────────────────────────────────────────────────────────
 then: write_tables --graphstore → serving table; flush names.idx
```

Each CU's entries are streamed straight into a **LevelDB GraphStore (which
dedups on disk)** and the per-CU file is **deleted immediately**. A bounded
channel applies backpressure, so at most a couple of GB of entry files are
ever on disk at once.

```bash
scry3 index-stream \
  --kzip aosp-norm.kzip \
  --out aosp.serving \
  --in frameworks/base,frameworks/native,system/,art/,libcore/ \
  --langs cxx,java,jvm --jvm-heap 12g --workers 24
# → aosp.serving/  (LevelDB) + aosp.serving/scry3.names.idx
# GraphStore scratch is created at aosp.serving.graphstore and deleted at the end.
```

### Measured (smoke: one C++ CU)

| | `index`+`build` | `index-stream` |
|---|---|---|
| peak RAM | low | **2.05 GB** |
| peak disk (entries) | 864 MB for 1 CU → multi-TB at scale | **GraphStore 0.2 GB**, entries never accumulate |
| output | serving + names | identical serving + names |

At AOSP-slice scale this turns "needs multiple TB" into "needs the GraphStore
(~tens of GB) + the serving table" — and peak RAM stays in single-digit GB,
**better than scry2's ~103 GB**.

## Disk budget for an AOSP slice (≈24k CUs)

| item | est. size | lifetime |
|---|---|---|
| GraphStore (deduped) | ~tens of GB | transient (deleted after build) |
| serving table | ~100–200 GB (≈10× scry2's `.s2db`; stock Kythe is data-rich) | the artifact |
| in-flight entry files | a few GB | transient (bounded channel) |
| name index | ~hundreds of MB | the artifact |

So an AOSP slice fits comfortably in a few hundred GB. The serving table is
the dominant cost and is inherent to stock Kythe's rich serving format — the
price of snippets/decorations/all-edge-kinds. Full AOSP scales up
proportionally; use `--in` to scope to the layers worth querying.

## Resume (`--resume`)

`index-stream` resumes a killed run seamlessly, like scry2's `from-kzip
--resume`:

```bash
scry3 index-stream --kzip aosp-norm.kzip --out aosp.serving \
    --in frameworks/base,... --langs cxx,java,jvm --jvm-heap 12g --workers 24 \
    --resume
```

Mechanics: as each CU is folded into the GraphStore, its sha is appended to
`<graphstore>.done` and its names to `<graphstore>.names`. On `--resume` the
existing GraphStore is reused, the `.done` shas are skipped, and the names are
preloaded — so the final name index stays complete. Folding into the
GraphStore is idempotent (re-folding a CU is a no-op), so a crash between
fold and log is safe. Pair with `--keep-graphstore` so the GraphStore (and
its resume state) survives until you've confirmed the run.

> The plain `index` path also resumes — it just skips any CU whose
> `<sha>.entries` already exists.

## Caveats

* The serving-table size estimate is extrapolated from one CU; measure on a
  ~100-CU sample before committing to a full run.
* `--resume` keeps the GraphStore between runs; pass `--keep-graphstore` on
  the first run (it's deleted on a clean finish unless you keep it).
