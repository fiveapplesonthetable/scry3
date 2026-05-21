# Kythe patches for AOSP cross-CU Java resolution

These four patches apply against the
[Kythe v0.0.75 release tag](https://github.com/kythe/kythe/releases/tag/v0.0.75).
Together they let `jvm_indexer.jar` read Java 21 bytecode and let
`java_indexer.jar` resolve classpath-bytecode references when the
CompilationUnit ships no `JavaDetails` proto extension (the AOSP norm).

Without them, `services.core → framework.jar.Binder.clearCallingIdentity`
returns 0 hits because the `/kythe/edge/named` bridge edge never fires.

For pure C++/Go/proto corpora the stock v0.0.75 indexers work as-is;
these patches are only needed for AOSP Java + JVM cross-CU coverage.

## Apply + build

```bash
git clone --depth=1 -b v0.0.75 https://github.com/kythe/kythe.git
cd kythe
git apply /path/to/scry2/kythe-patches/000{1,2,3,4}-*.patch
bazel run @unpinned_maven//:pin             # refresh maven_install.json after 0001
bazel build \
    //kythe/java/com/google/devtools/kythe/analyzers/java:indexer \
    //kythe/java/com/google/devtools/kythe/analyzers/jvm:indexer
```

Outputs land at:

* `bazel-bin/kythe/java/com/google/devtools/kythe/analyzers/java/indexer_deploy.jar`
* `bazel-bin/kythe/java/com/google/devtools/kythe/analyzers/jvm/indexer_deploy.jar`

Drop them into your Kythe release as `indexers/java_indexer.jar` and
`indexers/jvm_indexer.jar`. The other indexers (cxx, go, proto,
textproto) and the `tools/` directory are unmodified.

`scripts/install-aosp.sh` automates all of this.

## Order matters

* **0001** — `external.bzl`: bumps the ASM Maven dep `9.1 → 9.7.1`.
  Needed first; class-file major versions ≥ 61 (Java 17) require
  ASM 9.x to parse, and the upgrade-then-API-bump ordering is what
  Bazel's Maven pin expects.
* **0002** — `KytheClassVisitor.java`: `ASM_API_LEVEL = ASM9`
  (was `ASM7`). ASM 9 understands records, sealed classes, pattern
  matching for switch — every JEP that's now Java-21 stable. Without
  this, jvm_indexer throws on Java 17+ class-file features even with
  the dep upgrade.
* **0003** — `ClassFileIndexer.java`: new `--default_corpus` flag.
  On raw `.jar` / `.class` inputs, stock `jvm_indexer` emits VNames
  with `corpus=""`. `java_indexer`'s `named`-edge targets carry the
  build's real corpus; the two never merge in `write_tables`. The
  new flag lets the operator align them explicitly.
* **0004** — `CompilationUnitPathFileManager.java`: derive
  `StandardLocation.CLASS_PATH` from `!CLASS_PATH_JAR!`-prefixed
  `required_input` entries when `JavaDetails` is absent on the CU.
  **Load-bearing.** Empirically 0 → 1209 `named` edges to
  `android.os.Binder.*` JVM FQNs land after this patch on AOSP's
  services.core CU.

## Upstream-able

All four are upstream-shaped: minimal, scope-limited, no AOSP-specific
strings or scry2-specific wiring. The bigger ones (0003 and 0004) add
flags / fallbacks that are useful for any Kythe consumer dealing with
classpath bytecode jars or kzips emitted without `JavaDetails`. They
should be PR'd upstream when bandwidth allows.

## License

These patches modify Apache-2.0 code (Kythe). The diffs themselves are
distributed under Apache-2.0 as well. Keep the upstream `LICENSE` file
in any redistribution that includes the patched binaries.
