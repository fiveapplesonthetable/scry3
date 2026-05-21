//! `scry3 build` — turn a directory of `<sha>.entries` files into a Kythe
//! LevelDB **serving table** using the stock `write_tables` tool. This is
//! the back half scry2 replaced with its own `.s2db`; scry3 keeps it stock.
//!
//! Three modes, because the right one depends on corpus size:
//!
//! * `sorted` (default) — pipe every entry through `entrystream --unique`
//!   (sort + dedup in one pass) into a single GraphStore-ordered file, then
//!   `write_tables --entries`. Fastest for a scoped slice; the sort is
//!   in-process to `entrystream`, so peak RAM grows with the entry count.
//!
//! * `graphstore` — stream every entry into a LevelDB GraphStore via
//!   `write_entries` (sorts + dedups on disk in bounded RAM), then
//!   `write_tables --graphstore`. The memory-safe path for **all of AOSP**.
//!
//! * `beam` — `write_tables --experimental_beam_pipeline` reads the entries
//!   directory directly via the local disksort runner. This is the Google-
//!   scale path, but on a single machine the disksort runner is slow; only
//!   reach for it if you have a real Beam runner.

use anyhow::{bail, Context, Result};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Instant;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Mode {
    Sorted,
    Graphstore,
    Beam,
}

impl Mode {
    pub fn parse(s: &str) -> Result<Mode> {
        match s {
            "sorted" => Ok(Mode::Sorted),
            "graphstore" | "gs" => Ok(Mode::Graphstore),
            "beam" => Ok(Mode::Beam),
            _ => bail!("--mode: expected sorted|graphstore|beam, got {s:?}"),
        }
    }
}

pub struct BuildArgs<'a> {
    pub entries_dir: &'a Path,
    pub out: &'a Path,
    pub kythe_root: &'a Path,
    pub mode: Mode,
    pub workers: usize,
    /// Scratch dir for the intermediate sorted file / GraphStore.
    pub work: Option<&'a Path>,
    pub keep_intermediate: bool,
}

fn tool(kythe_root: &Path, name: &str) -> Result<PathBuf> {
    let p = kythe_root.join("tools").join(name);
    if !p.exists() {
        bail!("{} not found under {}", name, kythe_root.join("tools").display());
    }
    Ok(p)
}

/// Enumerate every `*.entries` file under `dir` (one level deep — that's
/// how `scry3 index` lays them out).
fn entry_files(dir: &Path) -> Result<Vec<PathBuf>> {
    let mut v = Vec::new();
    for e in std::fs::read_dir(dir).with_context(|| format!("read_dir {}", dir.display()))? {
        let p = e?.path();
        if p.extension().map(|x| x == "entries").unwrap_or(false) {
            v.push(p);
        }
    }
    v.sort();
    if v.is_empty() {
        bail!("no *.entries files in {}", dir.display());
    }
    Ok(v)
}

/// Stream the raw bytes of every entry file into `sink` (a child's stdin).
fn feed_entries(files: &[PathBuf], sink: &mut impl Write) -> Result<u64> {
    let mut total = 0u64;
    for f in files {
        let mut r = std::fs::File::open(f).with_context(|| format!("open {}", f.display()))?;
        total += std::io::copy(&mut r, sink).with_context(|| format!("feed {}", f.display()))?;
    }
    Ok(total)
}

pub fn run(args: BuildArgs<'_>) -> Result<()> {
    let t0 = Instant::now();
    if args.out.exists() {
        bail!(
            "--out {} already exists; remove it first (write_tables wants a fresh dir)",
            args.out.display()
        );
    }
    let work = args.work.map(|p| p.to_path_buf()).unwrap_or_else(|| {
        let base = std::env::var_os("SCRY_TMP_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| std::path::PathBuf::from("/mnt/agent/tmp"));
        base.join(format!("scry3-build-{}", std::process::id()))
    });
    std::fs::create_dir_all(&work).with_context(|| format!("mkdir work {}", work.display()))?;

    let files = entry_files(args.entries_dir)?;
    eprintln!("[build] {} entry files, mode={:?}", files.len(), args.mode);

    match args.mode {
        Mode::Sorted => build_sorted(&args, &files, &work)?,
        Mode::Graphstore => build_graphstore(&args, &files, &work)?,
        Mode::Beam => build_beam(&args)?,
    }

    eprintln!(
        "[build] done in {:.1}s → serving table at {}",
        t0.elapsed().as_secs_f64(),
        args.out.display()
    );
    Ok(())
}

fn build_sorted(args: &BuildArgs<'_>, files: &[PathBuf], work: &Path) -> Result<()> {
    let entrystream = tool(args.kythe_root, "entrystream")?;
    let write_tables = tool(args.kythe_root, "write_tables")?;
    let sorted = work.join("all.sorted.entries");

    eprintln!("[build] sort+dedup → {}", sorted.display());
    let out_f = std::fs::File::create(&sorted)
        .with_context(|| format!("create {}", sorted.display()))?;
    let mut child = Command::new(&entrystream)
        .arg("--unique") // implies --sort; dedups across CUs
        .stdin(Stdio::piped())
        .stdout(Stdio::from(out_f))
        .spawn()
        .context("spawn entrystream --unique")?;
    {
        let mut sink = child.stdin.take().unwrap();
        let n = feed_entries(files, &mut sink)?;
        eprintln!("[build] fed {:.2} GB of entries to entrystream", n as f64 / 1e9);
    }
    let st = child.wait().context("wait entrystream")?;
    if !st.success() {
        bail!("entrystream --unique failed: {st}");
    }

    eprintln!("[build] write_tables --entries → {}", args.out.display());
    let st = Command::new(&write_tables)
        .arg("--entries")
        .arg(&sorted)
        .arg("--out")
        .arg(args.out)
        .status()
        .context("spawn write_tables")?;
    if !st.success() {
        bail!("write_tables failed: {st}");
    }
    if !args.keep_intermediate {
        let _ = std::fs::remove_file(&sorted);
    }
    Ok(())
}

fn build_graphstore(args: &BuildArgs<'_>, files: &[PathBuf], work: &Path) -> Result<()> {
    let write_entries = tool(args.kythe_root, "write_entries")?;
    let write_tables = tool(args.kythe_root, "write_tables")?;
    let gs = work.join("graphstore");
    let workers = if args.workers == 0 { 4 } else { args.workers };

    eprintln!("[build] write_entries → GraphStore {} (workers={workers})", gs.display());
    let mut child = Command::new(&write_entries)
        .arg("--graphstore")
        .arg(&gs)
        .arg("--workers")
        .arg(workers.to_string())
        .stdin(Stdio::piped())
        .spawn()
        .context("spawn write_entries")?;
    {
        let mut sink = child.stdin.take().unwrap();
        let n = feed_entries(files, &mut sink)?;
        eprintln!("[build] fed {:.2} GB of entries to write_entries", n as f64 / 1e9);
    }
    let st = child.wait().context("wait write_entries")?;
    if !st.success() {
        bail!("write_entries failed: {st}");
    }

    eprintln!("[build] write_tables --graphstore → {}", args.out.display());
    let st = Command::new(&write_tables)
        .arg("--graphstore")
        .arg(&gs)
        .arg("--out")
        .arg(args.out)
        .status()
        .context("spawn write_tables")?;
    if !st.success() {
        bail!("write_tables failed: {st}");
    }
    if !args.keep_intermediate {
        let _ = std::fs::remove_dir_all(&gs);
    }
    Ok(())
}

fn build_beam(args: &BuildArgs<'_>) -> Result<()> {
    let write_tables = tool(args.kythe_root, "write_tables")?;
    // Beam's ReadEntries takes a directory when the path ends in a slash.
    let mut dir = args.entries_dir.as_os_str().to_os_string();
    dir.push("/");
    eprintln!("[build] write_tables --experimental_beam_pipeline (disksort)");
    let st = Command::new(&write_tables)
        .arg("--experimental_beam_pipeline")
        .arg("--entries")
        .arg(dir)
        .arg("--out")
        .arg(args.out)
        .status()
        .context("spawn write_tables (beam)")?;
    if !st.success() {
        bail!("write_tables (beam) failed: {st}");
    }
    Ok(())
}
