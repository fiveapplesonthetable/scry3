# scry3 build pipeline — kzip → serving table

End to end: **kzip → entries → serving table (+ name index) → queries**,
using stock Kythe for everything but the name index.

## Stage 1 — `scry3 index`: kzip → entries

Each compilation unit is extracted to a one-CU sub-kzip and fed to the
matching **patched** Kythe indexer in its own subprocess; the indexer's
stdout (a delimited `kythe.proto.Entry` stream) is written verbatim to
`entries/<sha>.entries`. This is scry2's `from-kzip` dispatch with the
storage half removed — the entries directory is the durable artifact, so a
killed run resumes by skipping CUs whose `<sha>.entries` already exists
(`--resume`).

Routing is by `v_name.language`: `c++`→cxx_indexer, `java`→java_indexer.jar,
`jvm`→jvm_indexer.jar, `go`→go_indexer, `protobuf`/`textproto`→their
indexers. Per-CU AFDO profile flags are stripped (cxx_indexer hard-fails on
them) and `--inject-cu-arg` rules add things like libcore's
`--patch-module=java.base=…`.

**The patches are required**, exactly as for scry2 — they patch the
*indexer*, not the storage, so they apply no matter what serves the data:
ASM 9.7.1 (Java 21), `--default_corpus` on jvm_indexer, and the
`CompilationUnitPathFileManager` classpath derivation that turns 0 → 1209
`named` edges to `android.os.Binder.*`. See `kythe-patches/`.

## Stage 2 — `scry3 build`: entries → serving table

This is stock `write_tables`. The real decision is **how** to run it.

### The decision: non-beam, for a single machine

`write_tables` has two pipelines:

* **non-beam** (`pipeline.Run`) — a purpose-built single-machine streaming
  pipeline. Input must be in GraphStore order, so scry3 either sorts the
  entries (`entrystream --unique`, mode `sorted`) or streams them into a
  LevelDB GraphStore first (`write_entries`, mode `graphstore`).
* **beam** (`--experimental_beam_pipeline`) — an Apache Beam pipeline. On a
  single machine it falls back to the local **disksort** runner.

Measured on one C++ CU (3.06 M entries, 839 MB):

| mode | wall | result |
|---|---|---|
| **non-beam** (`sorted`: entrystream --unique → write_tables --entries) | 38 s sort + 83 s build = **~2 min** | 519 MB serving table ✓ |
| beam (`--experimental_beam_pipeline`, disksort) | **>8 min, killed** | never finished on one CU |

The beam local runner is built for a distributed cluster; its single-machine
disksort fallback is unusably slow. **Default to non-beam.** Pick the
sub-mode by corpus size:

| corpus | mode | why |
|---|---|---|
| a scoped slice (recommended; `index --in …`) | `sorted` (default) | one sort+dedup pass, no second LevelDB write; fastest |
| **all of AOSP** | `graphstore` | `write_entries` sorts+dedups into LevelDB in **bounded RAM**; `sorted`'s in-`entrystream` sort would balloon to the full entry-set size in memory |
| you actually have a Beam/Dataflow runner | `beam` | only then |

So for "all of AOSP" the answer to *"LevelDB or beam?"* is: **LevelDB
GraphStore (`--mode graphstore`)** — it's the memory-safe single-machine
path. Beam is the wrong tool without a cluster.

### What the serving table contains (and doesn't)

`write_tables` builds `decor:` (file decorations), `edgeSets:`/`edgePages:`
(graph), and `xrefs:` (cross-references). It does **not** build the
identifier/search index — that table is Google-internal and never written by
the OSS pipeline. Hence Stage 3.

## Stage 3 — `scry3 name-index`: entries → names.idx

Streams every entry through stock `entrystream --write_format=json` and
scans the JSON for the two name carriers:

* `/kythe/edge/named` — target signature is the qualified name
  (Java/JVM/Go); the JVM method descriptor is stripped so both
  `pkg.Cls.m()V` and `pkg.Cls.m` resolve.
* `/kythe/code` — base64 MarkedSource proto (C++); rendered to a flat FQN
  like `android::Parcel::writeStrongBinder` by scry2's parser.

Output is a sorted `name<TAB>ticket` text file. Query verbs binary-search it
to turn a human name into the ticket(s) `kythe` needs.

## Stage 4 — query

`scry3 def/ref/callers` resolve the name to ticket(s) via names.idx, then
call stock `kythe xrefs` and reshape the JSON into terse `path:line:col
snippet` lines. `edges`/`nodes`/`decor`/`ls` pass through to `kythe`.
`--json` returns the raw kythe reply.
