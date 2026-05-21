//! `scry3 index-stream` — the AOSP-scale path: index → GraphStore → serving
//! table in **bounded disk**, never materializing the full entry set.
//!
//! The plain `index` + `build` path writes every CU's entries to disk before
//! building. Those entries are *not* deduplicated across CUs (every CU
//! re-emits all its shared header symbols), so for an AOSP-scale corpus the
//! entries directory balloons to multiple TB. scry2 sidesteps this by
//! streaming each CU into an in-RAM builder — trading disk for ~100 GB of
//! RAM. scry3 takes the third option here: **stream each CU's entries
//! straight into a LevelDB GraphStore (which dedups on disk) and delete the
//! per-CU file immediately.** Peak disk collapses to GraphStore + serving
//! table + a bounded handful of in-flight entry files; peak RAM stays low.
//!
//! Pipeline:
//! ```text
//!   N indexer workers ──(bounded channel, backpressure)──▶ 1 folder
//!     extract sub-kzip                                       batch files
//!     run indexer → tmp .entries                             write_entries → GraphStore
//!     scan names (shared set)                                delete batch
//!   ────────────────────────────────────────────────────────────────────
//!   then: write_tables --graphstore → serving table; flush names.idx
//! ```

use anyhow::{bail, Context, Result};
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::sync_channel;
use std::sync::Mutex;
use std::time::Instant;

use crate::indexer::{
    build_indexer_command, drain_tail, lang_label, num_cpus, route_language, CleanupPath,
    IndexerKind, InjectRule,
};
use crate::kzip;
use crate::nameindex;

const STDERR_TAIL_BYTES: usize = 4096;
/// Flush a fold batch to the GraphStore when it reaches this many files …
const BATCH_FILES: usize = 48;
/// … or this many bytes, whichever first. Bounds in-flight disk.
const BATCH_BYTES: u64 = 2 << 30; // 2 GiB

pub struct StreamArgs<'a> {
    pub kzip: &'a Path,
    pub kythe_root: &'a Path,
    /// Output serving table directory.
    pub out: &'a Path,
    /// Output name index. None = skip name-index build.
    pub names: Option<&'a Path>,
    /// GraphStore scratch dir (LevelDB). Removed at the end unless kept.
    pub graphstore: &'a Path,
    pub langs: &'a str,
    pub jvm_heap: &'a str,
    pub in_: &'a [String],
    pub not_in: &'a [String],
    pub staging: Option<&'a Path>,
    pub workers: usize,
    pub inject_rules: &'a [InjectRule],
    pub keep_graphstore: bool,
    /// Continue a killed run: reuse the existing GraphStore and skip CUs
    /// already recorded in `<graphstore>.done`.
    pub resume: bool,
}

/// Append the folded CUs' shas (entry-file stems) to the durable done log.
/// Folding into the GraphStore is idempotent, so a crash between fold and
/// log just re-folds the CU on resume — safe.
fn append_done(done_path: &Path, batch: &[PathBuf]) -> Result<()> {
    use std::io::Write;
    let mut f = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(done_path)
        .with_context(|| format!("open {}", done_path.display()))?;
    for p in batch {
        if let Some(stem) = p.file_stem().and_then(|s| s.to_str()) {
            writeln!(f, "{stem}")?;
        }
    }
    f.flush()?;
    Ok(())
}

#[derive(Default)]
struct Stats {
    ok: usize,
    empty: usize,
    failed: usize,
    folded: usize,
    fail_tails: Vec<String>,
}

/// In-memory (name,ticket) set plus a durable append-log behind it.
struct NameSink {
    set: BTreeSet<(String, String)>,
    file: Option<std::io::BufWriter<std::fs::File>>,
}
impl NameSink {
    /// Insert new pairs and append the genuinely-new ones to the durable log.
    fn add(&mut self, pairs: BTreeSet<(String, String)>) {
        use std::io::Write;
        for (n, t) in pairs {
            if self.set.insert((n.clone(), t.clone())) {
                if let Some(f) = self.file.as_mut() {
                    let _ = writeln!(f, "{n}\t{t}");
                }
            }
        }
    }
}

fn dir_size(p: &Path) -> u64 {
    std::fs::read_dir(p)
        .map(|rd| {
            rd.filter_map(|e| e.ok())
                .filter_map(|e| e.metadata().ok().map(|m| m.len()))
                .sum()
        })
        .unwrap_or(0)
}

/// Fold a batch of entry files into the GraphStore with one `write_entries`
/// call (delimited proto streams concatenate cleanly), then delete them.
fn fold_batch(write_entries: &Path, gs: &Path, files: &[PathBuf], gs_workers: usize) -> Result<()> {
    if files.is_empty() {
        return Ok(());
    }
    let mut child = Command::new(write_entries)
        .arg("--graphstore")
        .arg(gs)
        .arg("--workers")
        .arg(gs_workers.to_string())
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .context("spawn write_entries")?;
    {
        let mut sink = child.stdin.take().unwrap();
        for f in files {
            let mut r = std::fs::File::open(f).with_context(|| format!("open {}", f.display()))?;
            std::io::copy(&mut r, &mut sink).with_context(|| format!("feed {}", f.display()))?;
        }
    }
    let tail = child
        .stderr
        .take()
        .map(|h| drain_tail(h, STDERR_TAIL_BYTES))
        .unwrap_or_default();
    let st = child.wait().context("wait write_entries")?;
    if !st.success() {
        bail!("write_entries failed ({st}): {}", tail.lines().last().unwrap_or(""));
    }
    for f in files {
        let _ = std::fs::remove_file(f);
    }
    Ok(())
}

pub fn run(args: StreamArgs<'_>) -> Result<()> {
    let t0 = Instant::now();
    if args.out.exists() {
        bail!("--out {} already exists; remove it first", args.out.display());
    }
    let done_path = {
        let mut p = args.graphstore.as_os_str().to_os_string();
        p.push(".done");
        PathBuf::from(p)
    };
    let mut done_shas: std::collections::HashSet<String> = std::collections::HashSet::new();
    if args.graphstore.exists() {
        if !args.resume {
            bail!("--graphstore {} already exists; pass --resume to continue, or remove it",
                args.graphstore.display());
        }
        if let Ok(s) = std::fs::read_to_string(&done_path) {
            for line in s.lines() {
                let t = line.trim();
                if !t.is_empty() {
                    done_shas.insert(t.to_string());
                }
            }
        }
        eprintln!("[stream] --resume: reusing graphstore, {} CUs already folded", done_shas.len());
    }
    let write_entries = args.kythe_root.join("tools/write_entries");
    let write_tables = args.kythe_root.join("tools/write_tables");
    let entrystream = args.kythe_root.join("tools/entrystream");
    for t in [&write_entries, &write_tables, &entrystream] {
        if !t.exists() {
            bail!("missing tool {}", t.display());
        }
    }

    // ---- plan -------------------------------------------------------------
    let want: BTreeSet<&str> = args.langs.split(',').map(|s| s.trim()).collect();
    let in_f = args.in_;
    let not_f = args.not_in;
    let accept = |p: &str| -> bool {
        if !in_f.is_empty() && !in_f.iter().any(|s| !s.is_empty() && p.contains(s.as_str())) {
            return false;
        }
        !not_f.iter().any(|s| !s.is_empty() && p.contains(s.as_str()))
    };
    eprintln!("[stream] reading {} …", args.kzip.display());
    let units = if in_f.is_empty() && not_f.is_empty() {
        kzip::read_units_progress(args.kzip, kzip::NoProgress)?
    } else {
        kzip::read_units_filtered(args.kzip, kzip::NoProgress, accept)?
    };
    let mut plan: Vec<(IndexerKind, &kzip::Unit)> = Vec::new();
    for u in &units {
        let Some(kind) = route_language(&u.language()) else { continue };
        if !want.contains(lang_label(kind)) {
            continue;
        }
        if !accept(u.primary_path().unwrap_or("")) {
            continue;
        }
        plan.push((kind, u));
    }
    if !done_shas.is_empty() {
        let before = plan.len();
        plan.retain(|(_, u)| !done_shas.contains(&u.sha));
        eprintln!("[stream] --resume: skipped {} already-folded CUs", before - plan.len());
    }
    eprintln!("[stream] plan: {} CUs", plan.len());
    if plan.is_empty() {
        // Everything already folded; fall through to write_tables on the
        // existing graphstore (still need the serving table + name index).
        eprintln!("[stream] all CUs already folded; building serving table from graphstore");
    }

    let staging = args.staging.map(|p| p.to_path_buf()).unwrap_or_else(|| {
        std::env::var_os("SCRY_TMP_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| std::path::PathBuf::from("/mnt/agent/tmp"))
            .join(format!("scry3-stream-{}", std::process::id()))
    });
    std::fs::create_dir_all(&staging)?;
    let workers = if args.workers == 0 { std::cmp::max(1, num_cpus() / 2) } else { args.workers };
    eprintln!("[stream] workers={workers} staging={}", staging.display());

    // ---- shared state -----------------------------------------------------
    let want_names = args.names.is_some();
    // Durable name sink: the (name,ticket) set is also appended to
    // `<graphstore>.names` as it grows, and preloaded from there on --resume,
    // so a killed run never loses the names of already-folded CUs.
    let names_durable = {
        let mut p = args.graphstore.as_os_str().to_os_string();
        p.push(".names");
        PathBuf::from(p)
    };
    let name_sink: Mutex<NameSink> = {
        let mut set = BTreeSet::new();
        let mut file = None;
        if want_names {
            if let Ok(s) = std::fs::read_to_string(&names_durable) {
                for line in s.lines() {
                    if let Some((n, t)) = line.split_once('\t') {
                        set.insert((n.to_string(), t.to_string()));
                    }
                }
                if !set.is_empty() {
                    eprintln!("[stream] --resume: preloaded {} name rows", set.len());
                }
            }
            let f = std::fs::OpenOptions::new()
                .create(true).append(true).open(&names_durable)
                .with_context(|| format!("open {}", names_durable.display()))?;
            file = Some(std::io::BufWriter::new(f));
        }
        Mutex::new(NameSink { set, file })
    };
    let stats = Mutex::new(Stats::default());
    let done = AtomicUsize::new(0);
    let plan_total = plan.len();
    // Bounded channel: at most ~2× workers entry files awaiting fold. With
    // BATCH bounds this caps in-flight disk to a few GB regardless of corpus.
    let (tx, rx) = sync_channel::<PathBuf>(workers * 2);

    let plan_ref = &plan;
    let stats_ref = &stats;
    let names_ref = &name_sink;
    let done_ref = &done;
    let done_path_ref = &done_path;

    std::thread::scope(|s| -> Result<()> {
        // ---- folder (single GraphStore writer) ----
        let gs = args.graphstore;
        let gs_path = args.graphstore;
        let we = &write_entries;
        let staging_ref = &staging;
        let folder = s.spawn(move || -> Result<usize> {
            let mut batch: Vec<PathBuf> = Vec::with_capacity(BATCH_FILES);
            let mut batch_bytes = 0u64;
            let mut folded = 0usize;
            let mut last_report = Instant::now();
            while let Ok(path) = rx.recv() {
                batch_bytes += std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
                batch.push(path);
                if batch.len() >= BATCH_FILES || batch_bytes >= BATCH_BYTES {
                    fold_batch(we, gs, &batch, 4)?;
                    append_done(done_path_ref, &batch)?;
                    folded += batch.len();
                    batch.clear();
                    batch_bytes = 0;
                    if last_report.elapsed().as_secs() >= 15 {
                        eprintln!(
                            "[stream] folded {folded} CUs; graphstore={:.1} GB; staging={:.1} GB",
                            dir_size(gs_path) as f64 / 1e9,
                            dir_size(staging_ref) as f64 / 1e9,
                        );
                        last_report = Instant::now();
                    }
                }
            }
            fold_batch(we, gs, &batch, 4)?;
            append_done(done_path_ref, &batch)?;
            folded += batch.len();
            Ok(folded)
        });

        // ---- indexer workers ----
        for w_id in 0..workers {
            let tx = tx.clone();
            let staging = staging.clone();
            let kythe_root = args.kythe_root;
            let jvm_heap = args.jvm_heap.to_string();
            let inject_rules = args.inject_rules;
            let entrystream = &entrystream;
            s.spawn(move || {
                let mut extractor = match kzip::SubKzipWriter::open(args.kzip) {
                    Ok(e) => e,
                    Err(e) => {
                        eprintln!("[stream] worker {w_id}: {e:#}");
                        return;
                    }
                };
                let mut i = w_id;
                while i < plan_ref.len() {
                    let (kind, unit) = plan_ref[i];
                    i += workers;
                    let sub = staging.join(format!("{}.kzip", unit.sha));
                    let jvm_tmp = staging.join(format!("{}.jvmtmp", unit.sha));
                    let ent = staging.join(format!("{}.entries", unit.sha));
                    let _g1 = CleanupPath { path: sub.clone(), is_dir: false };
                    let _g2 = matches!(kind, IndexerKind::JavaSource | IndexerKind::JvmBytecode)
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
                    let fail = |msg: String| {
                        let mut st = stats_ref.lock().unwrap();
                        st.failed += 1;
                        if st.fail_tails.len() < 8 {
                            st.fail_tails.push(msg);
                        }
                    };
                    if let Err(e) = extractor.extract_with(unit, &sub, |cu| {
                        cu.argument.retain(|a| !a.starts_with("-fprofile-sample-use"));
                        for &a in matching.iter().rev() {
                            if !cu.argument.iter().any(|x| x == a) {
                                cu.argument.insert(0, a.to_string());
                            }
                        }
                    }) {
                        fail(format!("sha={} extract: {e:#}", unit.sha));
                        done_ref.fetch_add(1, Ordering::Relaxed);
                        continue;
                    }
                    let mut cmd = match build_indexer_command(kind, kythe_root, &sub, &jvm_heap, &jvm_tmp) {
                        Ok(c) => c,
                        Err(e) => {
                            fail(format!("sha={} indexer: {e:#}", unit.sha));
                            done_ref.fetch_add(1, Ordering::Relaxed);
                            continue;
                        }
                    };
                    let out_f = match std::fs::File::create(&ent) {
                        Ok(f) => f,
                        Err(e) => {
                            fail(format!("sha={} create: {e:#}", unit.sha));
                            done_ref.fetch_add(1, Ordering::Relaxed);
                            continue;
                        }
                    };
                    let child = cmd.stdout(Stdio::from(out_f)).stderr(Stdio::piped()).spawn();
                    let mut child = match child {
                        Ok(c) => c,
                        Err(e) => {
                            let _ = std::fs::remove_file(&ent);
                            fail(format!("sha={} spawn: {e:#}", unit.sha));
                            done_ref.fetch_add(1, Ordering::Relaxed);
                            continue;
                        }
                    };
                    let stderr_h = child.stderr.take().unwrap();
                    let tail_t = std::thread::spawn(move || drain_tail(stderr_h, STDERR_TAIL_BYTES));
                    let status = child.wait();
                    let tail = tail_t.join().unwrap_or_default();
                    let ok = matches!(&status, Ok(s) if s.success());
                    let nbytes = std::fs::metadata(&ent).map(|m| m.len()).unwrap_or(0);
                    done_ref.fetch_add(1, Ordering::Relaxed);
                    if !ok {
                        let _ = std::fs::remove_file(&ent);
                        let snip: String = tail.lines().rev().take(2).collect::<Vec<_>>().join(" | ");
                        fail(format!("sha={} {snip}", unit.sha));
                        continue;
                    }
                    if nbytes == 0 {
                        let _ = std::fs::remove_file(&ent);
                        stats_ref.lock().unwrap().empty += 1;
                        continue;
                    }
                    // Names: scan and DURABLY persist before handing the file
                    // to the folder (which deletes it) — so a CU's names are on
                    // disk before it can be folded, keeping --resume complete.
                    if want_names {
                        let mut cu_names = BTreeSet::new();
                        let _ = nameindex::scan_file(entrystream, &ent, &mut cu_names);
                        if !cu_names.is_empty() {
                            names_ref.lock().unwrap().add(cu_names);
                        }
                    }
                    stats_ref.lock().unwrap().ok += 1;
                    // Backpressure: blocks if the folder is behind, bounding
                    // in-flight disk. If the folder died, the send errors and
                    // we stop.
                    if tx.send(ent).is_err() {
                        break;
                    }
                }
            });
        }
        drop(tx); // only worker clones remain; folder disconnects when they finish

        // Light progress from the main scope thread while work proceeds.
        let folded = folder.join().map_err(|_| anyhow::anyhow!("folder thread panicked"))??;
        stats_ref.lock().unwrap().folded = folded;
        let _ = done_ref.load(Ordering::Relaxed);
        let _ = plan_total;
        Ok(())
    })?;

    let st = stats.into_inner().unwrap();
    eprintln!(
        "[stream] indexed: ok={} empty={} failed={}; folded {} CUs into graphstore ({:.1} GB)",
        st.ok,
        st.empty,
        st.failed,
        st.folded,
        dir_size(args.graphstore) as f64 / 1e9
    );
    for t in &st.fail_tails {
        eprintln!("[stream]   ! {t}");
    }

    // ---- write_tables: graphstore → serving table -------------------------
    eprintln!("[stream] write_tables --graphstore → {}", args.out.display());
    let wt = Command::new(&write_tables)
        .arg("--graphstore")
        .arg(args.graphstore)
        .arg("--out")
        .arg(args.out)
        .status()
        .context("spawn write_tables")?;
    if !wt.success() {
        bail!("write_tables failed: {wt}");
    }

    // ---- name index -------------------------------------------------------
    if let Some(npath) = args.names {
        let sink = name_sink.into_inner().unwrap();
        nameindex::write_index(&sink.set, npath)?;
        eprintln!("[stream] name index: {} rows → {}", sink.set.len(), npath.display());
    }

    if !args.keep_graphstore {
        let _ = std::fs::remove_dir_all(args.graphstore);
        let _ = std::fs::remove_file(&done_path);
        let _ = std::fs::remove_file(&names_durable);
    }
    let _ = std::fs::remove_dir_all(&staging);
    eprintln!(
        "[stream] done in {:.1} min → serving table {} ({:.1} GB)",
        t0.elapsed().as_secs_f64() / 60.0,
        args.out.display(),
        dir_size(args.out) as f64 / 1e9,
    );
    Ok(())
}
