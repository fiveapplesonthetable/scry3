//! The one piece stock Kythe's open-source serving table is missing: a
//! name → ticket index. Kythe's `IdentifierMatch` table (what
//! `kythe identifier` reads) is only ever *read* in the OSS release —
//! nothing in `write_tables` writes it. So `scry3` builds its own.
//!
//! Build path stays "stock as much as possible": we shell every entry
//! through `entrystream --write_format=json` (Kythe owns the wire decode)
//! and scan the JSON for the two name carriers — `/kythe/edge/named`
//! (Java/JVM/Go) and `/kythe/code` MarkedSource (C++). The output is a
//! plain sorted text file, `name\tticket` per line, deduped. Small, greppable,
//! and binary-searchable.

use anyhow::{bail, Context, Result};
use serde::Deserialize;
use std::collections::BTreeSet;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Mutex;
use std::time::Instant;

use crate::marked_source::{
    base64_decode, is_named_edge, parse_marked_source_fqn, strip_jvm_method_descriptor,
};
use crate::ticket::VName;

#[derive(Deserialize)]
struct JsonEntry {
    #[serde(default)]
    source: VName,
    #[serde(default)]
    target: Option<VName>,
    #[serde(default)]
    edge_kind: String,
    #[serde(default)]
    fact_name: String,
    #[serde(default)]
    fact_value: String,
}

fn entrystream_tool(kythe_root: &Path) -> Result<PathBuf> {
    let p = kythe_root.join("tools/entrystream");
    if !p.exists() {
        bail!("entrystream not found at {}", p.display());
    }
    Ok(p)
}

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

/// Scan one entries file for (name, ticket) pairs. Shared by the standalone
/// `name-index` builder and the streaming `index-stream` pipeline.
pub(crate) fn scan_file(
    entrystream: &Path,
    file: &Path,
    out: &mut BTreeSet<(String, String)>,
) -> Result<()> {
    let f = std::fs::File::open(file).with_context(|| format!("open {}", file.display()))?;
    let mut child = Command::new(entrystream)
        .arg("--write_format=json")
        .stdin(Stdio::from(f))
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .context("spawn entrystream")?;
    let stdout = child.stdout.take().unwrap();
    let rdr = BufReader::with_capacity(1 << 20, stdout);
    for line in rdr.lines() {
        let line = match line {
            Ok(l) => l,
            Err(_) => break,
        };
        if line.is_empty() {
            continue;
        }
        let e: JsonEntry = match serde_json::from_str(&line) {
            Ok(e) => e,
            Err(_) => continue,
        };
        // Java/JVM/Go: the `named` edge target signature is the FQN.
        if is_named_edge(&e.edge_kind) {
            if let Some(t) = &e.target {
                if !t.signature.is_empty() {
                    let ticket = e.source.to_ticket();
                    if let Some(stripped) = strip_jvm_method_descriptor(&t.signature) {
                        out.insert((stripped.to_string(), ticket.clone()));
                    }
                    out.insert((t.signature.clone(), ticket));
                }
            }
            continue;
        }
        // C++: the human name is a MarkedSource proto under /kythe/code.
        if e.fact_name == "/kythe/code" && !e.fact_value.is_empty() {
            if let Some(bytes) = base64_decode(&e.fact_value) {
                if let Some(fqn) = parse_marked_source_fqn(&bytes) {
                    out.insert((fqn, e.source.to_ticket()));
                }
            }
        }
    }
    let _ = child.wait();
    Ok(())
}

pub struct NameIndexArgs<'a> {
    pub entries_dir: &'a Path,
    pub out: &'a Path,
    pub kythe_root: &'a Path,
    pub workers: usize,
}

pub fn build(args: NameIndexArgs<'_>) -> Result<()> {
    let t0 = Instant::now();
    let entrystream = entrystream_tool(args.kythe_root)?;
    let files = entry_files(args.entries_dir)?;
    eprintln!("[name-index] scanning {} entry files", files.len());

    let workers = if args.workers == 0 {
        std::cmp::max(1, std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1))
    } else {
        args.workers
    };
    let merged: Mutex<BTreeSet<(String, String)>> = Mutex::new(BTreeSet::new());
    let files_ref = &files;
    let merged_ref = &merged;
    let entrystream_ref = &entrystream;
    std::thread::scope(|s| {
        for w in 0..workers {
            s.spawn(move || {
                let mut local = BTreeSet::new();
                let mut i = w;
                while i < files_ref.len() {
                    if let Err(e) = scan_file(entrystream_ref, &files_ref[i], &mut local) {
                        eprintln!("[name-index] warn: {} — {e:#}", files_ref[i].display());
                    }
                    i += workers;
                }
                merged_ref.lock().unwrap().extend(local);
            });
        }
    });

    let set = merged.into_inner().unwrap();
    write_index(&set, args.out)?;
    eprintln!(
        "[name-index] {} (name,ticket) pairs → {} in {:.1}s",
        set.len(),
        args.out.display(),
        t0.elapsed().as_secs_f64()
    );
    Ok(())
}

/// Write a sorted `name<TAB>ticket` index file. The set is already in
/// `(name, ticket)` order — exactly what binary-search lookup wants.
pub(crate) fn write_index(set: &BTreeSet<(String, String)>, out: &Path) -> Result<()> {
    let f = std::fs::File::create(out).with_context(|| format!("create {}", out.display()))?;
    let mut w = std::io::BufWriter::new(f);
    for (name, ticket) in set {
        writeln!(w, "{name}\t{ticket}")?;
    }
    w.flush()?;
    Ok(())
}

/// In-memory view of a name index for lookups. Lines are pre-sorted by the
/// builder, so exact lookup is a binary search.
pub struct NameIndex {
    rows: Vec<(String, String)>,
}

impl NameIndex {
    pub fn load(path: &Path) -> Result<NameIndex> {
        let data = std::fs::read_to_string(path)
            .with_context(|| format!("read name index {}", path.display()))?;
        let mut rows = Vec::new();
        for line in data.lines() {
            if let Some((n, t)) = line.split_once('\t') {
                rows.push((n.to_string(), t.to_string()));
            }
        }
        // Defensive: the file should already be sorted, but a hand-edited or
        // concatenated index might not be.
        if !rows.windows(2).all(|w| w[0] <= w[1]) {
            rows.sort();
        }
        Ok(NameIndex { rows })
    }

    /// Number of (name, ticket) rows.
    pub fn len(&self) -> usize {
        self.rows.len()
    }

    /// True when the index holds no rows.
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// Tickets whose name equals `name` exactly.
    pub fn exact(&self, name: &str) -> Vec<String> {
        let lo = self.rows.partition_point(|(n, _)| n.as_str() < name);
        let mut out = Vec::new();
        for (n, t) in &self.rows[lo..] {
            if n != name {
                break;
            }
            out.push(t.clone());
        }
        out
    }

    /// Tickets whose name contains `needle` (linear scan). Capped at `limit`
    /// distinct names to keep output bounded.
    pub fn substr(&self, needle: &str, limit: usize) -> Vec<(String, String)> {
        let mut out = Vec::new();
        for (n, t) in &self.rows {
            if n.contains(needle) {
                out.push((n.clone(), t.clone()));
                if out.len() >= limit {
                    break;
                }
            }
        }
        out
    }
}
