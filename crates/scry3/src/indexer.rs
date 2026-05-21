//! `scry3 index` — kzip → per-CU Kythe `Entry` streams on disk.
//!
//! This is a deliberately *thinner* fork of scry2's `from-kzip`. scry2
//! decoded each indexer's stdout into its own packed `.s2db`; scry3 keeps
//! the indexer's stdout verbatim — a delimited `kythe.proto.Entry` stream,
//! which is exactly what `write_tables` consumes. So the whole storage
//! half (IndexBuilder / snapshots / streaming merge) is gone. What stays
//! is the part that's genuinely hard and worth reusing:
//!
//!   * per-CU sub-kzip extraction (`kzip::SubKzipWriter`),
//!   * language → indexer routing,
//!   * the AFDO-profile-flag strip and `--inject-cu-arg` transforms,
//!   * crash isolation (one bad CU never sinks the batch),
//!   * parallel dispatch.
//!
//! The output is a directory of `<sha>.entries` files. That directory is
//! the durable artifact: a killed run resumes simply by skipping every CU
//! whose `<sha>.entries` already exists.

use anyhow::{Context, Result};
use std::collections::{HashMap, HashSet};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use std::time::Instant;

use crate::kzip;

/// Routing from a CU's `v_name.language` to an indexer binary. `None`
/// means "no indexer in Kythe v0.0.75 for this language" (kotlin / rust
/// source); those CUs are counted as skipped, not failed.
#[derive(Clone, Copy, Debug)]
pub enum IndexerKind {
    Cxx,
    JavaSource,
    JvmBytecode,
    Go,
    Proto,
    TextProto,
}

pub fn route_language(lang: &str) -> Option<IndexerKind> {
    match lang {
        "c++" => Some(IndexerKind::Cxx),
        "java" => Some(IndexerKind::JavaSource),
        "jvm" => Some(IndexerKind::JvmBytecode),
        "go" => Some(IndexerKind::Go),
        "protobuf" | "proto" => Some(IndexerKind::Proto),
        "textproto" => Some(IndexerKind::TextProto),
        _ => None,
    }
}

pub fn lang_label(k: IndexerKind) -> &'static str {
    match k {
        IndexerKind::Cxx => "cxx",
        IndexerKind::JavaSource => "java",
        IndexerKind::JvmBytecode => "jvm",
        IndexerKind::Go => "go",
        IndexerKind::Proto => "proto",
        IndexerKind::TextProto => "textproto",
    }
}

/// One `--inject-cu-arg PREFIX::ARG` rule. When a CU's primary path starts
/// with `path_prefix`, prepend `arg` to its compiler argv (skipped if
/// already present). Mirrors scry2's semantics so the AOSP wrapper's
/// libcore `--patch-module` rule keeps working unchanged.
#[derive(Debug, Clone)]
pub struct InjectRule {
    pub path_prefix: String,
    pub arg: String,
}

pub fn parse_inject_rules(raw: &[String]) -> Result<Vec<InjectRule>> {
    let mut out = Vec::with_capacity(raw.len());
    for r in raw {
        let (p, a) = r.split_once("::").ok_or_else(|| {
            anyhow::anyhow!("--inject-cu-arg: missing `::` separator in {r:?}; expected PREFIX::ARG")
        })?;
        if p.is_empty() {
            anyhow::bail!("--inject-cu-arg: empty PREFIX in {r:?}");
        }
        if a.is_empty() {
            anyhow::bail!("--inject-cu-arg: empty ARG in {r:?}");
        }
        out.push(InjectRule { path_prefix: p.into(), arg: a.into() });
    }
    Ok(out)
}

pub(crate) fn build_indexer_command(
    kind: IndexerKind,
    kythe_root: &Path,
    cu_kzip: &Path,
    jvm_heap: &str,
    jvm_temp_dir: &Path,
) -> Result<Command> {
    match kind {
        IndexerKind::Cxx => {
            let bin = kythe_root.join("indexers/cxx_indexer");
            if !bin.exists() {
                anyhow::bail!("cxx_indexer missing: {}", bin.display());
            }
            let mut c = Command::new(bin);
            c.arg(cu_kzip);
            Ok(c)
        }
        IndexerKind::Go => {
            let bin = kythe_root.join("indexers/go_indexer");
            if !bin.exists() {
                anyhow::bail!("go_indexer missing: {}", bin.display());
            }
            let mut c = Command::new(bin);
            c.arg(cu_kzip);
            Ok(c)
        }
        IndexerKind::JavaSource | IndexerKind::JvmBytecode => {
            let jar = kythe_root.join(if matches!(kind, IndexerKind::JavaSource) {
                "indexers/java_indexer.jar"
            } else {
                "indexers/jvm_indexer.jar"
            });
            if !jar.exists() {
                anyhow::bail!("{} missing", jar.display());
            }
            let mut c = Command::new("java");
            c.arg(format!("-Xmx{jvm_heap}"))
                .arg("-jar")
                .arg(jar)
                .arg("--ignore_empty_kzip")
                .arg("--temp_directory")
                .arg(jvm_temp_dir)
                .arg(cu_kzip);
            Ok(c)
        }
        IndexerKind::Proto => {
            let bin = kythe_root.join("indexers/proto_indexer");
            if !bin.exists() {
                anyhow::bail!("proto_indexer missing: {}", bin.display());
            }
            let mut c = Command::new(bin);
            c.arg(format!("-index_file={}", cu_kzip.display()));
            Ok(c)
        }
        IndexerKind::TextProto => {
            let bin = kythe_root.join("indexers/textproto_indexer");
            if !bin.exists() {
                anyhow::bail!("textproto_indexer missing: {}", bin.display());
            }
            let mut c = Command::new(bin);
            c.arg(format!("--index_file={}", cu_kzip.display()));
            Ok(c)
        }
    }
}

#[derive(Default, Debug, Clone)]
struct LangStats {
    cus: usize,
    succeeded: usize,
    empty: usize,
    failed: usize,
    bytes: u64,
    fail_tails: Vec<String>,
}

const MAX_FAIL_TAILS: usize = 8;
const STDERR_TAIL_BYTES: usize = 4096;

/// Read `r` to EOF, keeping only the last `cap` bytes (for failure diag).
pub(crate) fn drain_tail<R: Read>(mut r: R, cap: usize) -> String {
    let mut buf = Vec::with_capacity(cap.min(1 << 16));
    let mut chunk = [0u8; 8192];
    loop {
        match r.read(&mut chunk) {
            Ok(0) | Err(_) => break,
            Ok(n) => {
                buf.extend_from_slice(&chunk[..n]);
                if buf.len() > cap {
                    let cut = buf.len() - cap;
                    buf.drain(0..cut);
                }
            }
        }
    }
    String::from_utf8_lossy(&buf).into_owned()
}

/// RAII: remove a path (file or dir) on drop, including on panic.
pub(crate) struct CleanupPath {
    pub(crate) path: PathBuf,
    pub(crate) is_dir: bool,
}
impl Drop for CleanupPath {
    fn drop(&mut self) {
        if self.is_dir {
            let _ = std::fs::remove_dir_all(&self.path);
        } else {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

pub(crate) fn num_cpus() -> usize {
    std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1)
}

pub struct IndexArgs<'a> {
    pub kzip: &'a Path,
    pub kythe_root: &'a Path,
    pub entries_dir: &'a Path,
    pub langs: &'a str,
    pub jvm_heap: &'a str,
    pub in_: &'a [String],
    pub not_in: &'a [String],
    pub staging: Option<&'a Path>,
    pub workers: usize,
    pub inject_rules: &'a [InjectRule],
    pub resume: bool,
}

pub fn run(args: IndexArgs<'_>) -> Result<()> {
    let t0 = Instant::now();
    let want: HashSet<&str> = args.langs.split(',').map(|s| s.trim()).collect();

    std::fs::create_dir_all(args.entries_dir)
        .with_context(|| format!("mkdir entries dir {}", args.entries_dir.display()))?;

    eprintln!("[index] reading {} …", args.kzip.display());
    let in_filters = args.in_;
    let not_in_filters = args.not_in;
    let accept_path = |p: &str| -> bool {
        if !in_filters.is_empty()
            && !in_filters.iter().any(|s| !s.is_empty() && p.contains(s.as_str()))
        {
            return false;
        }
        if not_in_filters.iter().any(|s| !s.is_empty() && p.contains(s.as_str())) {
            return false;
        }
        true
    };

    let units = if in_filters.is_empty() && not_in_filters.is_empty() {
        kzip::read_units_progress(args.kzip, kzip::NoProgress)?
    } else {
        kzip::read_units_filtered(args.kzip, kzip::NoProgress, accept_path)?
    };
    eprintln!("[index] {} units after path filter", units.len());

    let mut plan: Vec<(IndexerKind, &kzip::Unit)> = Vec::with_capacity(units.len());
    let (mut skipped_lang, mut skipped_path, mut skipped_done) = (0usize, 0usize, 0usize);
    for u in &units {
        let Some(kind) = route_language(&u.language()) else {
            skipped_lang += 1;
            continue;
        };
        if !want.contains(lang_label(kind)) {
            skipped_lang += 1;
            continue;
        }
        if !accept_path(u.primary_path().unwrap_or("")) {
            skipped_path += 1;
            continue;
        }
        if args.resume {
            let out = args.entries_dir.join(format!("{}.entries", u.sha));
            if std::fs::metadata(&out).map(|m| m.len() > 0).unwrap_or(false) {
                skipped_done += 1;
                continue;
            }
        }
        plan.push((kind, u));
    }
    eprintln!(
        "[index] plan: {} CUs ({} skipped: lang={}, path={}, already-done={})",
        plan.len(),
        skipped_lang + skipped_path + skipped_done,
        skipped_lang,
        skipped_path,
        skipped_done,
    );
    if plan.is_empty() {
        eprintln!("[index] nothing to do");
        return Ok(());
    }

    let staging = args.staging.map(|p| p.to_path_buf()).unwrap_or_else(|| {
        let base = std::env::var_os("SCRY_TMP_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| std::env::temp_dir());
        base.join(format!("scry3-index-{}", std::process::id()))
    });
    std::fs::create_dir_all(&staging)
        .with_context(|| format!("mkdir staging {}", staging.display()))?;
    eprintln!("[index] staging: {}", staging.display());

    let workers = if args.workers == 0 {
        std::cmp::max(1, num_cpus() / 2)
    } else {
        args.workers
    };
    eprintln!("[index] workers: {workers}");

    let by_lang: Mutex<HashMap<&'static str, LangStats>> = Mutex::new(HashMap::new());
    let done = AtomicUsize::new(0);
    let plan_total = plan.len();
    let plan_ref = &plan;
    let by_lang_ref = &by_lang;
    let done_ref = &done;
    let run_start = Instant::now();

    std::thread::scope(|s| {
        for w_id in 0..workers {
            let staging = staging.clone();
            let inject_rules = args.inject_rules;
            let kythe_root = args.kythe_root;
            let jvm_heap = args.jvm_heap.to_string();
            let entries_dir = args.entries_dir;
            let kzip_path = args.kzip;
            s.spawn(move || -> Result<()> {
                let mut extractor = kzip::SubKzipWriter::open(kzip_path)?;
                let mut i = w_id;
                while i < plan_ref.len() {
                    let (kind, unit) = plan_ref[i];
                    i += workers;
                    let label = lang_label(kind);
                    let sub_path = staging.join(format!("{}.kzip", unit.sha));
                    let jvm_tmp = staging.join(format!("{}.jvmtmp", unit.sha));
                    let out_tmp = entries_dir.join(format!("{}.entries.tmp", unit.sha));
                    let out_final = entries_dir.join(format!("{}.entries", unit.sha));
                    let _sub_guard = CleanupPath { path: sub_path.clone(), is_dir: false };
                    let _jvm_guard = matches!(
                        kind,
                        IndexerKind::JavaSource | IndexerKind::JvmBytecode
                    )
                    .then(|| {
                        let _ = std::fs::create_dir_all(&jvm_tmp);
                        CleanupPath { path: jvm_tmp.clone(), is_dir: true }
                    });

                    let primary = unit.primary_path().unwrap_or("").to_string();
                    let matching: Vec<&str> = inject_rules
                        .iter()
                        .filter(|r| primary.starts_with(&r.path_prefix))
                        .map(|r| r.arg.as_str())
                        .collect();

                    let record_fail = |msg: String| {
                        let mut bl = by_lang_ref.lock().unwrap();
                        let st = bl.entry(label).or_default();
                        st.cus += 1;
                        st.failed += 1;
                        if st.fail_tails.len() < MAX_FAIL_TAILS {
                            st.fail_tails.push(msg);
                        }
                    };

                    // Strip codegen-only AFDO profile flags (point at files
                    // not in the kzip; cxx_indexer hard-fails on them) and
                    // apply inject rules.
                    let extract_res = extractor.extract_with(unit, &sub_path, |cu| {
                        cu.argument.retain(|a| !a.starts_with("-fprofile-sample-use"));
                        for &a in matching.iter().rev() {
                            if !cu.argument.iter().any(|e| e == a) {
                                cu.argument.insert(0, a.to_string());
                            }
                        }
                    });
                    if let Err(e) = extract_res {
                        record_fail(format!("sha={} extract: {e:#}", unit.sha));
                        done_ref.fetch_add(1, Ordering::Relaxed);
                        continue;
                    }

                    let mut cmd = match build_indexer_command(
                        kind, kythe_root, &sub_path, &jvm_heap, &jvm_tmp,
                    ) {
                        Ok(c) => c,
                        Err(e) => {
                            record_fail(format!("sha={} indexer: {e:#}", unit.sha));
                            done_ref.fetch_add(1, Ordering::Relaxed);
                            continue;
                        }
                    };
                    let out_f = match std::fs::File::create(&out_tmp) {
                        Ok(f) => f,
                        Err(e) => {
                            record_fail(format!("sha={} create out: {e:#}", unit.sha));
                            done_ref.fetch_add(1, Ordering::Relaxed);
                            continue;
                        }
                    };
                    // Indexer stdout (the Entry stream) goes straight to the
                    // file — no copy through this process. stderr is piped so
                    // we can keep a tail for diagnosis.
                    let mut child = match cmd
                        .stdout(Stdio::from(out_f))
                        .stderr(Stdio::piped())
                        .spawn()
                    {
                        Ok(c) => c,
                        Err(e) => {
                            let _ = std::fs::remove_file(&out_tmp);
                            record_fail(format!("sha={} spawn: {e:#}", unit.sha));
                            done_ref.fetch_add(1, Ordering::Relaxed);
                            continue;
                        }
                    };
                    let stderr_h = child.stderr.take().unwrap();
                    let stderr_thread =
                        std::thread::spawn(move || drain_tail(stderr_h, STDERR_TAIL_BYTES));
                    let status = child.wait();
                    let tail = stderr_thread.join().unwrap_or_default();

                    let ok = matches!(&status, Ok(s) if s.success());
                    let nbytes = std::fs::metadata(&out_tmp).map(|m| m.len()).unwrap_or(0);
                    if !ok {
                        let _ = std::fs::remove_file(&out_tmp);
                        let code = status
                            .as_ref()
                            .map(|s| s.code().map(|c| c.to_string()).unwrap_or_else(|| "signal".into()))
                            .unwrap_or_else(|_| "spawn-err".into());
                        let snippet: String = tail.lines().rev().take(2).collect::<Vec<_>>().join(" | ");
                        record_fail(format!("sha={} exit={code} {snippet}", unit.sha));
                    } else if nbytes == 0 {
                        let _ = std::fs::remove_file(&out_tmp);
                        let mut bl = by_lang_ref.lock().unwrap();
                        let st = bl.entry(label).or_default();
                        st.cus += 1;
                        st.empty += 1;
                    } else {
                        let _ = std::fs::rename(&out_tmp, &out_final);
                        let mut bl = by_lang_ref.lock().unwrap();
                        let st = bl.entry(label).or_default();
                        st.cus += 1;
                        st.succeeded += 1;
                        st.bytes += nbytes;
                    }
                    let n = done_ref.fetch_add(1, Ordering::Relaxed) + 1;
                    if n % 200 == 0 || n == plan_total {
                        let el = run_start.elapsed().as_secs_f64();
                        let rate = if el > 1.0 { n as f64 * 60.0 / el } else { 0.0 };
                        eprintln!("[index] {n}/{plan_total} ({rate:.0}/min) +{el:.0}s");
                    }
                }
                Ok(())
            });
        }
    });

    // The `.thread::scope` join is implicit at the end of the closure; any
    // worker error surfaces via its Result, but a panicking worker would
    // abort the scope. We summarize whatever landed.
    let bl = by_lang.lock().unwrap();
    let mut tot = LangStats::default();
    eprintln!("[index] ── per-language summary ──");
    let mut langs: Vec<_> = bl.keys().copied().collect();
    langs.sort_unstable();
    for l in langs {
        let st = &bl[l];
        eprintln!(
            "[index]   {l:<9} cus={} ok={} empty={} failed={} bytes={:.1}M",
            st.cus, st.succeeded, st.empty, st.failed, st.bytes as f64 / 1e6
        );
        for t in &st.fail_tails {
            eprintln!("[index]       ! {t}");
        }
        tot.succeeded += st.succeeded;
        tot.empty += st.empty;
        tot.failed += st.failed;
        tot.bytes += st.bytes;
    }
    eprintln!(
        "[index] done in {:.1}s → {} ({} ok, {} empty, {} failed, {:.2} GB entries)",
        t0.elapsed().as_secs_f64(),
        args.entries_dir.display(),
        tot.succeeded,
        tot.empty,
        tot.failed,
        tot.bytes as f64 / 1e9,
    );
    Ok(())
}
