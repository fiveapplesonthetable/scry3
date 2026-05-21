# scry3 on AOSP — end to end

Assumes you already have an AOSP kzip (from `build/soong/build_kzip.bash`,
normalized to single-encoding if needed — see scry2's `normalize-kzip`) and a
Kythe release with the **patched** indexers (`kythe-patches/`).

```bash
export KYTHE_ROOT=~/scry2-setup/kythe-v0.0.75      # patched java/jvm jars
KZIP=~/aosp/out/dist/aosp-norm.kzip
WORK=~/scry3-aosp

# 1. kzip → entries  (scoped to the layers worth querying; resumable)
scry3 index --kzip "$KZIP" --out-entries "$WORK/entries" \
    --in frameworks/base,frameworks/native,system/,art/,libcore/ \
    --inject-cu-arg 'libcore/ojluni/src/main/java/::--patch-module=java.base=libcore/ojluni/src/main/java' \
    --jvm-heap 12g --resume

# 2. entries → serving table.
#    For a full unfiltered AOSP corpus use --mode graphstore (memory-safe).
#    For a scoped slice the default --mode sorted is faster.
scry3 build --entries "$WORK/entries" --out "$WORK/serving" --mode graphstore

# 3. entries → name index (the bit stock Kythe's OSS serving table lacks)
scry3 name-index --entries "$WORK/entries" --out "$WORK/serving/scry3.names.idx"

# 4. query  (NAME resolves via the name index; tickets pass through)
scry3 --serving "$WORK/serving" def     android.os.Binder.clearCallingIdentity
scry3 --serving "$WORK/serving" callers android.os.Binder.clearCallingIdentity
scry3 --serving "$WORK/serving" ref     android::Parcel::writeStrongBinder
scry3 --serving "$WORK/serving" edges   android.os.Binder --kinds /kythe/edge/extends
scry3 --serving "$WORK/serving" nodes   '<ticket>'
```

`--name-index` defaults to `<serving>/scry3.names.idx`, so once it lives there
the query verbs need only `--serving`.

## Notes specific to AOSP

* **Patches are mandatory for Java/JVM cross-CU.** Without them, Java queries
  return 0 cross-CU hits — the `/kythe/edge/named` bridge never fires. Same
  root cause as scry2; the patches are in `kythe-patches/`.
* **Mixed-encoding kzip.** `build_kzip.bash` emits both `pbunits/` and
  `units/`. Stock indexers crash on mixed encoding; normalize first
  (scry2 ships `normalize-kzip`; scry3's `index` reads units directly via the
  same kzip reader and tolerates both, but normalizing once up front is the
  safe path for the rest of the stock toolchain).
* **Disk.** Budget tens of GB for the serving table on a full corpus, plus a
  GraphStore of similar size during `--mode graphstore` (removed afterwards
  unless `--keep-intermediate`).
* **`build --mode beam` is a trap on a single machine** — see
  [`PIPELINE.md`](PIPELINE.md). Use `graphstore`.
