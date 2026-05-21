//! Kythe `.kzip` walker, decoder, and normalizer.
//!
//! ## Why this exists
//!
//! Real AOSP `.kzip` archives (the output of `build_kzip.bash`) mix
//! two unit encodings — `root/pbunits/<sha>` (proto-encoded
//! `IndexedCompilation`) and `root/units/<sha>` (proto3-JSON of the
//! same message). Stock Kythe v0.0.75 indexers and the `kzip` tool
//! refuse mixed-encoding kzips with `Malformed kzip: multiple unit
//! encodings but different entries` and abort hard. We need to
//! handle 100% of the units.
//!
//! ## Approach
//!
//! Hand-rolled proto wire codec + serde-JSON decoder for the few
//! `kythe.proto.*` messages we touch (`IndexedCompilation`,
//! `CompilationUnit`, `FileInput`, `FileInfo`, `VName`). No protobuf
//! codegen dependency, no `build.rs`, no third-party proto schema —
//! these messages are stable Kythe public API and the wire format is
//! self-documenting.
//!
//! ## What we expose
//!
//! * [`read_units`] — iterate every CU in the source kzip, regardless
//!   of encoding.
//! * [`Unit::to_proto_bytes`] — re-encode a decoded CU as proto, so
//!   downstream code can write single-encoding sub-kzips.
//! * [`normalize`] — one-shot helper that takes a mixed-encoding kzip
//!   and writes a fresh proto-encoded kzip with every file blob
//!   preserved verbatim.

use anyhow::{anyhow, bail, Context, Result};
use serde::Deserialize;
use std::collections::HashSet;
use std::fs::File;
use std::io::{BufWriter, Read, Write};
use std::path::Path;

// ----------------------------------------------------------------- types

/// Subset of `kythe.proto.VName` we need.
#[derive(Default, Debug, Clone, Deserialize)]
#[serde(default)]
pub struct VName {
    pub signature: String,
    pub corpus:    String,
    pub root:      String,
    pub path:      String,
    pub language:  String,
}

#[derive(Default, Debug, Clone, Deserialize)]
#[serde(default)]
pub struct FileInfo {
    pub path:   String,
    pub digest: String,
}

#[derive(Default, Debug, Clone, Deserialize)]
#[serde(default)]
pub struct FileInput {
    #[serde(alias = "vName")]
    pub v_name: VName,
    pub info:   FileInfo,
    // Other FileInput fields (source_context, context, details) are
    // round-tripped as raw bytes on the proto path; for JSON input we
    // currently drop them and let the indexer fall back to defaults.
}

#[derive(Default, Debug, Clone, Deserialize)]
#[serde(default)]
pub struct CompilationUnit {
    #[serde(alias = "vName")]
    pub v_name: VName,
    #[serde(alias = "requiredInput")]
    pub required_input: Vec<FileInput>,
    pub argument: Vec<String>,
    #[serde(alias = "sourceFile")]
    pub source_file: Vec<String>,
    #[serde(alias = "outputKey")]
    pub output_key: String,
    #[serde(alias = "workingDirectory")]
    pub working_directory: String,
    #[serde(alias = "entryContext")]
    pub entry_context: String,
    // `details` is a repeated google.protobuf.Any; we copy the raw
    // bytes through on the proto path. On the JSON path we skip it
    // (java_indexer's `JavaDetails` lives here, but for AOSP-shape
    // CUs without JavaDetails the patched fallback in
    // CompilationUnitPathFileManager kicks in anyway).
}

#[derive(Default, Debug, Clone, Deserialize)]
#[serde(default)]
pub struct IndexedCompilation {
    pub unit: CompilationUnit,
    // `index` and `file_data` are skipped on JSON input. The proto
    // encoder writes only `unit`.
}

/// One CU read out of the source kzip, plus the sha that named it
/// (so the writer can preserve hashing).
#[derive(Debug)]
pub struct Unit {
    pub sha:  String,
    pub cu:   IndexedCompilation,
    pub raw_proto: Option<Vec<u8>>,
}

impl Unit {
    /// Re-encode this unit as a proto-encoded `IndexedCompilation`.
    /// Returns the raw proto bytes (no varint length prefix).
    pub fn to_proto_bytes(&self) -> Vec<u8> {
        if let Some(raw) = &self.raw_proto { return raw.clone(); }
        encode_indexed_compilation(&self.cu)
    }

    /// Best guess at the CU's primary source path. Used by from-kzip
    /// for `--in` / `--not-in` filtering before we incur sub-kzip
    /// build + indexer spawn cost.
    ///
    /// Preference order:
    /// 1. `source_file[0]` — explicit primary file (set by Bazel-style
    ///    extractors and AOSP's Soong/xref_cxx).
    /// 2. `required_input[0].v_name.path` — the first input's VName
    ///    path (corpus-relative). Present on every Kythe CU.
    /// 3. `v_name.path` — the unit's own VName (rare but valid for
    ///    JVM bytecode units that have no source files).
    pub fn primary_path(&self) -> Option<&str> {
        cu_primary_path(&self.cu.unit)
    }

    /// Language hint from `v_name.language`. Lower-cased so callers
    /// can match without worrying about case (Kythe is consistent
    /// but some custom extractors aren't).
    pub fn language(&self) -> String {
        self.cu.unit.v_name.language.to_ascii_lowercase()
    }
}

/// Same lookup as [`Unit::primary_path`] but free-standing so callers
/// that hold only a `CompilationUnit` (no Unit wrapper, no raw_proto)
/// can re-check the path post-decode without rebuilding a Unit.
pub fn cu_primary_path(cu: &CompilationUnit) -> Option<&str> {
    if let Some(f) = cu.source_file.first() { return Some(f.as_str()); }
    if let Some(ri) = cu.required_input.first() {
        if !ri.v_name.path.is_empty() { return Some(&ri.v_name.path); }
        if !ri.info.path.is_empty()   { return Some(&ri.info.path);   }
    }
    let vp = cu.v_name.path.as_str();
    if !vp.is_empty() { Some(vp) } else { None }
}

// ----------------------------------------------------------------- reader

/// Progress sink: invoked periodically by long-running kzip ops.
/// `total = 0` is allowed if the caller doesn't know it yet (printer
/// can fall back to a spinner). Use [`NoProgress`] for silent runs;
/// `&mut P` works wherever `P: Progress` so a single sink can flow
/// through nested calls without re-wrapping.
pub trait Progress {
    fn report(&mut self, phase: &str, done: usize, total: usize);
}

/// Zero-cost no-op progress sink. Use when you don't care about
/// progress (tests, library callers, batch scripts).
pub struct NoProgress;
impl Progress for NoProgress {
    fn report(&mut self, _: &str, _: usize, _: usize) {}
}

impl<P: Progress + ?Sized> Progress for &mut P {
    fn report(&mut self, phase: &str, done: usize, total: usize) {
        (**self).report(phase, done, total)
    }
}

/// Iterate every CU in `path`, regardless of pbunits/units encoding.
/// Decoded results stream — peak memory is one buffered unit.
///
/// On encoding overlap (the same sha appearing under both pbunits/
/// and units/), the proto wins and the JSON twin is silently dropped.
pub fn read_units(path: &Path) -> Result<Vec<Unit>> {
    read_units_progress(path, NoProgress)
}

/// Cheap peek over raw unit bytes for the CU's primary source path,
/// used by [`read_units_filtered`] to skip the full decode when a
/// path filter would drop the unit anyway. Walks the proto/JSON
/// without allocating per-CU work. Returns:
///   `Some(path)` — confidently located the primary source path;
///   `Some("")`  — successfully parsed structure with no source path
///                 (e.g., file-only CUs);
///   `None`      — the peek could not find a recognizable structure;
///                 the caller should fall back to a full decode rather
///                 than silently drop the CU under an `--in` filter.
pub fn peek_primary_path(bytes: &[u8], is_proto: bool) -> Option<String> {
    if is_proto { peek_proto_primary_path(bytes) }
    else        { peek_json_primary_path(bytes)  }
}

fn peek_proto_primary_path(bytes: &[u8]) -> Option<String> {
    // IndexedCompilation.unit (field 1) is a sub-message.
    let cu = find_field_ld_truncating(bytes, 1)?;
    // CompilationUnit.source_file (field 6) is a repeated string —
    // take the first one if present.
    if let Some(s) = find_first_field_string(cu, 6) { return Some(s); }
    // Fall back to CompilationUnit.required_input[0] (field 3,
    // repeated FileInput). FileInput.v_name (field 1) is a VName,
    // its .path (field 4) is the source path.
    let first_input = find_field_ld_truncating(cu, 3)?;
    let vname       = find_field_ld_truncating(first_input, 1)?;
    if let Some(p) = find_first_field_string(vname, 4) { return Some(p); }
    // Last resort: FileInput.info (field 2) is FileInfo, its .path
    // (field 1) is the source path. If even that's absent we return
    // Some("") — the structure was parseable, the CU just has no
    // primary path. (Distinct from `None`, which means "couldn't
    // walk the proto at all".)
    let info = find_field_ld_truncating(first_input, 2)?;
    Some(find_first_field_string(info, 1).unwrap_or_default())
}

fn peek_json_primary_path(bytes: &[u8]) -> Option<String> {
    // Prefer the proto3-JSON name `source_file` (snake) or its
    // camelCase twin. CompilationUnit.source_file is a repeated
    // string so the value is `["path", ...]`; we peek the first
    // element. If the encoder happens to emit a bare string, take
    // that too.
    for key in [&b"\"source_file\""[..], &b"\"sourceFile\""[..]] {
        if let Some(s) = json_first_value_string_or_list(bytes, key) {
            return Some(s);
        }
    }
    // Walk every `"path":"<...>"` and return the first whose suffix
    // looks like a source file. Covers CUs whose source_file isn't a
    // direct top-level key but whose first source-extension required
    // input is the primary source.
    let mut cursor = 0;
    while let Some(rel) = memmem(&bytes[cursor..], b"\"path\"") {
        let after = cursor + rel + b"\"path\"".len();
        if let Some((val, end)) = json_take_string_value(bytes, after) {
            if is_source_extension(&val) { return Some(val); }
            cursor = end;
        } else {
            cursor = after;
        }
    }
    // Couldn't peek a recognizable source path — let the caller fall
    // back to a full decode rather than silently dropping the CU.
    None
}

/// Walk a length-delimited proto sub-message for one field, returning
/// its raw payload bytes. Truncating: when the declared length runs
/// past `bytes`, return the clipped tail anyway (peek buffers are
/// allowed to end mid-field).
fn find_field_ld_truncating(bytes: &[u8], target: u32) -> Option<&[u8]> {
    let mut cur = 0;
    while cur < bytes.len() {
        let (tag, n) = peek_varint(&bytes[cur..])?;
        cur += n;
        let field = (tag >> 3) as u32;
        let wire = (tag & 0x7) as u8;
        if field == target && wire == 2 {
            let (len, ln) = peek_varint(&bytes[cur..])?;
            cur += ln;
            let end = cur.checked_add(len as usize)?;
            return Some(&bytes[cur..end.min(bytes.len())]);
        }
        cur = skip_proto_field(bytes, cur, wire)?;
    }
    None
}

/// First length-delimited string field at `target`, decoded UTF-8.
fn find_first_field_string(bytes: &[u8], target: u32) -> Option<String> {
    let raw = find_field_ld_truncating(bytes, target)?;
    std::str::from_utf8(raw).ok().map(|s| s.to_string())
}

fn skip_proto_field(bytes: &[u8], cur: usize, wire: u8) -> Option<usize> {
    match wire {
        0 => { let (_, n) = peek_varint(&bytes[cur..])?; Some(cur + n) }
        1 => if cur + 8 <= bytes.len() { Some(cur + 8) } else { None },
        2 => {
            let (len, n) = peek_varint(&bytes[cur..])?;
            let end = cur.checked_add(n)?.checked_add(len as usize)?;
            if end > bytes.len() { return None; }
            Some(end)
        }
        5 => if cur + 4 <= bytes.len() { Some(cur + 4) } else { None },
        _ => None,
    }
}

/// Standalone varint reader used by the slice-walking peek helpers
/// above. Returns the decoded value plus the byte count it consumed,
/// or `None` if `bytes` is truncated. (Distinct from the cursor-style
/// [`read_varint_bytes`] used by the strict decoder further down.)
fn peek_varint(bytes: &[u8]) -> Option<(u64, usize)> {
    let mut val: u64 = 0;
    let mut shift = 0u32;
    for i in 0..10 {
        let b = *bytes.get(i)?;
        val |= ((b & 0x7F) as u64) << shift;
        if b & 0x80 == 0 { return Some((val, i + 1)); }
        shift += 7;
    }
    None
}

fn memmem(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() || haystack.len() < needle.len() { return None; }
    haystack.windows(needle.len()).position(|w| w == needle)
}

/// Locate `key` in `bytes` and return the first string value after
/// the `:` — accepting either a bare `"..."` or the first element of
/// a `[ "...", ... ]` array. Used to peek proto3-JSON repeated-string
/// fields like `source_file`.
fn json_first_value_string_or_list(bytes: &[u8], key: &[u8]) -> Option<String> {
    let rel = memmem(bytes, key)?;
    let after = rel + key.len();
    let mut p = after;
    while p < bytes.len() && bytes[p].is_ascii_whitespace() { p += 1; }
    if bytes.get(p) != Some(&b':') { return None; }
    p += 1;
    while p < bytes.len() && bytes[p].is_ascii_whitespace() { p += 1; }
    if bytes.get(p) == Some(&b'[') {
        p += 1;
        while p < bytes.len() && bytes[p].is_ascii_whitespace() { p += 1; }
    }
    if bytes.get(p) != Some(&b'"') { return None; }
    // Reuse json_take_string_value's quote-walking by reconstructing
    // a position just after the key — feed it the `:` we already
    // confirmed, but anchored at the same offset. The simpler path
    // is to inline the quote walk here.
    p += 1;
    let start = p;
    while p < bytes.len() && bytes[p] != b'"' {
        if bytes[p] == b'\\' && p + 1 < bytes.len() { p += 2; }
        else { p += 1; }
    }
    if p >= bytes.len() { return None; }
    let raw = &bytes[start..p];
    Some(std::str::from_utf8(raw).ok()?.to_string())
}

fn json_take_string_value(bytes: &[u8], from: usize) -> Option<(String, usize)> {
    let mut p = from;
    while p < bytes.len() && bytes[p].is_ascii_whitespace() { p += 1; }
    if bytes.get(p) != Some(&b':') { return None; }
    p += 1;
    while p < bytes.len() && bytes[p].is_ascii_whitespace() { p += 1; }
    if bytes.get(p) != Some(&b'"') { return None; }
    p += 1;
    let start = p;
    while p < bytes.len() && bytes[p] != b'"' {
        if bytes[p] == b'\\' && p + 1 < bytes.len() { p += 2; }
        else { p += 1; }
    }
    if p >= bytes.len() { return None; }
    let raw = &bytes[start..p];
    Some((std::str::from_utf8(raw).ok()?.to_string(), p + 1))
}

fn is_source_extension(path: &str) -> bool {
    const EXTS: &[&str] = &[
        ".cc", ".cpp", ".cxx", ".c++", ".c", ".m", ".mm",
        ".java", ".kt", ".go", ".rs",
        ".proto", ".textpb", ".textproto",
    ];
    EXTS.iter().any(|e| path.ends_with(e))
}

/// Read every CU in `path` whose primary source path satisfies
/// `accept`. When the cheap peek confidently locates a path, the CU
/// is dropped without a full decode — this avoids the ~3 min / 5 GB
/// cost of decoding 118 k AOSP units when only ~1 k of them match an
/// `--in` filter. When the peek returns `None` (unrecognized layout)
/// we fall back to a full decode and re-check `accept` against the
/// decoded primary path, so a malformed-but-otherwise-parseable CU is
/// never silently dropped.
pub fn read_units_filtered<P: Progress, F: Fn(&str) -> bool>(
    path: &Path, mut progress: P, accept: F,
) -> Result<Vec<Unit>> {
    let f = File::open(path).with_context(|| format!("open kzip {}", path.display()))?;
    let mut zip = zip::ZipArchive::new(f).with_context(|| "open zip")?;
    let mut pbunit_idx: Vec<(usize, String)> = Vec::new();
    let mut json_idx:   Vec<(usize, String)> = Vec::new();
    for i in 0..zip.len() {
        let name = zip.by_index(i)?.name().to_string();
        if let Some(sha) = strip_prefix(&name, "root/pbunits/") {
            pbunit_idx.push((i, sha.to_string()));
        } else if let Some(sha) = strip_prefix(&name, "root/units/") {
            json_idx.push((i, sha.to_string()));
        }
    }
    let total_units = pbunit_idx.len() + json_idx.len();
    let mut proto_shas: HashSet<String> = HashSet::new();
    let mut out: Vec<Unit> = Vec::new();
    let mut scanned = 0usize;
    for (i, sha) in &pbunit_idx {
        let mut f = zip.by_index(*i)?;
        let mut buf = Vec::with_capacity(f.size() as usize);
        f.read_to_end(&mut buf)?;
        scanned += 1;
        progress.report("scan", scanned, total_units);
        match peek_primary_path(&buf, true) {
            Some(p) if !accept(&p) => continue,
            _ => {}
        }
        let cu = parse_indexed_compilation(&buf)
            .with_context(|| format!("decode proto unit {sha}"))?;
        if !accept(cu_primary_path(&cu.unit).unwrap_or("")) { continue; }
        proto_shas.insert(sha.clone());
        out.push(Unit { sha: sha.clone(), cu, raw_proto: Some(buf) });
    }
    for (i, sha) in &json_idx {
        if proto_shas.contains(sha) { scanned += 1; continue; }
        let mut f = zip.by_index(*i)?;
        let mut buf = String::with_capacity(f.size() as usize);
        f.read_to_string(&mut buf)?;
        scanned += 1;
        progress.report("scan", scanned, total_units);
        match peek_primary_path(buf.as_bytes(), false) {
            Some(p) if !accept(&p) => continue,
            _ => {}
        }
        let cu: IndexedCompilation = serde_json::from_str(&buf)
            .with_context(|| format!("decode JSON unit {sha}"))?;
        if !accept(cu_primary_path(&cu.unit).unwrap_or("")) { continue; }
        out.push(Unit { sha: sha.clone(), cu, raw_proto: None });
    }
    Ok(out)
}

/// Same as [`read_units`] but invokes `progress.report("read", ...)`
/// on each unit decoded.
pub fn read_units_progress<P: Progress>(path: &Path, mut progress: P) -> Result<Vec<Unit>> {
    let f = File::open(path).with_context(|| format!("open kzip {}", path.display()))?;
    let mut zip = zip::ZipArchive::new(f).with_context(|| "open zip")?;
    // Walk the central directory once and bucket the per-prefix
    // indices. Two cheap wins:
    //   1. `total_units` is known before we decode anything, so the
    //      progress callback shows a real percentage from the first
    //      call.
    //   2. We skip the central-dir walk a second time. The previous
    //      naive form did `for i in 0..zip.len()` three times; on a
    //      59 GB AOSP kzip with ~700k entries that wasted ~60 s of
    //      wall time on a kzip whose JSON-units bucket was empty
    //      after normalize-kzip.
    let mut pbunit_idx: Vec<(usize, String)> = Vec::new();  // (zip-index, sha)
    let mut json_idx:   Vec<(usize, String)> = Vec::new();
    for i in 0..zip.len() {
        let name = zip.by_index(i)?.name().to_string();
        if let Some(sha) = strip_prefix(&name, "root/pbunits/") {
            pbunit_idx.push((i, sha.to_string()));
        } else if let Some(sha) = strip_prefix(&name, "root/units/") {
            json_idx.push((i, sha.to_string()));
        }
    }
    let total_units = pbunit_idx.len() + json_idx.len();
    let mut proto_shas: HashSet<String> = HashSet::new();
    let mut out: Vec<Unit> = Vec::with_capacity(total_units);
    // First pass: proto-encoded units (they win on collision).
    for (i, sha) in &pbunit_idx {
        let mut f = zip.by_index(*i)?;
        let mut buf = Vec::with_capacity(f.size() as usize);
        f.read_to_end(&mut buf)?;
        let cu = parse_indexed_compilation(&buf)
            .with_context(|| format!("decode proto unit {sha}"))?;
        proto_shas.insert(sha.clone());
        out.push(Unit { sha: sha.clone(), cu, raw_proto: Some(buf) });
        progress.report("read", out.len(), total_units);
    }
    // Second pass: JSON-encoded units, skipping any with a proto twin.
    for (i, sha) in &json_idx {
        if proto_shas.contains(sha) { continue; }
        let mut f = zip.by_index(*i)?;
        let mut buf = String::with_capacity(f.size() as usize);
        f.read_to_string(&mut buf)?;
        let cu: IndexedCompilation = serde_json::from_str(&buf)
            .with_context(|| format!("decode JSON unit {sha}"))?;
        out.push(Unit { sha: sha.clone(), cu, raw_proto: None });
        progress.report("read", out.len(), total_units);
    }
    Ok(out)
}

fn strip_prefix<'a>(name: &'a str, prefix: &str) -> Option<&'a str> {
    let after = name.strip_prefix(prefix)?;
    // Reject the directory entry itself (`root/units/` → after="") and
    // any deeper paths (a malformed kzip might contain them).
    if after.is_empty() || after.contains('/') { return None; }
    Some(after)
}

// ----------------------------------------------------------------- writer

/// Write a fresh proto-only kzip at `out`, carrying every unit from
/// `units` plus every required-input file blob from `src`. The
/// output kzip is structurally identical to what `build_kzip.bash`
/// produces with `KYTHE_KZIP_ENCODING=proto` — readable by every
/// stock Kythe v0.0.75 indexer.
pub fn write_normalized(src: &Path, units: &[Unit], out: &Path) -> Result<()> {
    write_normalized_progress(src, units, out, NoProgress)
}

/// Same as [`write_normalized`] but invokes `progress.report` with
/// phases `"write-units"` (proto re-encode + emit) and
/// `"write-files"` (file-blob carry-over).
pub fn write_normalized_progress<P: Progress>(
    src: &Path, units: &[Unit], out: &Path, mut progress: P,
) -> Result<()> {
    let in_f = File::open(src).with_context(|| format!("reopen kzip {}", src.display()))?;
    let mut zin = zip::ZipArchive::new(in_f)?;
    let out_f = File::create(out).with_context(|| format!("create {}", out.display()))?;
    let mut zout = zip::ZipWriter::new(BufWriter::with_capacity(8 << 20, out_f));
    let opts = zip::write::FileOptions::default()
        .compression_method(zip::CompressionMethod::Stored);

    // Required directory entries (Kythe spec).
    zout.add_directory("root/", opts)?;
    zout.add_directory("root/pbunits/", opts)?;
    zout.add_directory("root/files/", opts)?;

    // Units → proto.
    let mut needed_files: HashSet<String> = HashSet::new();
    for (i, u) in units.iter().enumerate() {
        zout.start_file(format!("root/pbunits/{}", u.sha), opts)?;
        zout.write_all(&u.to_proto_bytes())?;
        for fi in &u.cu.unit.required_input {
            if !fi.info.digest.is_empty() { needed_files.insert(fi.info.digest.clone()); }
        }
        progress.report("write-units", i + 1, units.len());
    }
    // File blobs — copy verbatim from the source kzip.
    let total_files = needed_files.len();
    for (i, digest) in needed_files.iter().enumerate() {
        let entry_name = format!("root/files/{digest}");
        let mut src_f = match zin.by_name(&entry_name) {
            Ok(f) => f,
            Err(_) => continue,  // some required_inputs reference files outside
                                  // the kzip (e.g. compiler builtins); skip.
        };
        zout.start_file(&entry_name, opts)?;
        std::io::copy(&mut src_f, &mut zout)?;
        progress.report("write-files", i + 1, total_files);
    }
    zout.finish()?.flush()?;
    Ok(())
}

/// Open the source kzip once and hand out per-CU sub-kzip extractions.
/// Avoids the ~50 ms central-directory rescan cost per call that you
/// pay if you `ZipArchive::new` for every unit — important for AOSP-
/// scale runs where we extract 100k+ sub-kzips.
pub struct SubKzipWriter {
    zin: zip::ZipArchive<File>,
}

impl SubKzipWriter {
    pub fn open(src: &Path) -> Result<Self> {
        let f = File::open(src).with_context(|| format!("reopen kzip {}", src.display()))?;
        Ok(Self { zin: zip::ZipArchive::new(f)? })
    }

    /// Write `dst` as a single-CU kzip: one proto-encoded pbunit +
    /// every `root/files/<digest>` blob the unit's required_input
    /// names. Equivalent in structure to what `build_kzip.bash`
    /// produces when run against just one compile target — every
    /// stock Kythe indexer accepts it as input.
    ///
    /// Returns the number of file blobs copied (some required_inputs
    /// reference compiler-builtin paths that aren't in the kzip; those
    /// are skipped silently, matching `write_normalized`).
    pub fn extract(&mut self, unit: &Unit, dst: &Path) -> Result<usize> {
        self.extract_with(unit, dst, |_| {})
    }

    /// Same as [`extract`] but applies `transform` to the
    /// `IndexedCompilation` before emitting the pbunit, letting the
    /// caller mutate per-CU args / source_file / etc. (e.g. inject a
    /// `--patch-module=java.base=…` for libcore CUs). When the
    /// transform mutates the unit, we re-encode from the decoded
    /// struct so changes actually land in the sub-kzip; otherwise we
    /// preserve the raw bytes verbatim.
    pub fn extract_with<F: FnOnce(&mut CompilationUnit)>(
        &mut self, unit: &Unit, dst: &Path, transform: F,
    ) -> Result<usize> {
        let out_f = File::create(dst).with_context(|| format!("create {}", dst.display()))?;
        let mut zout = zip::ZipWriter::new(BufWriter::with_capacity(1 << 20, out_f));
        let opts = zip::write::FileOptions::default()
            .compression_method(zip::CompressionMethod::Stored);
        zout.add_directory("root/", opts)?;
        zout.add_directory("root/pbunits/", opts)?;
        zout.add_directory("root/files/", opts)?;
        zout.start_file(format!("root/pbunits/{}", unit.sha), opts)?;
        // Snapshot args BEFORE/AFTER transform to detect actual mutation
        // — if the transform was a no-op we can keep raw_proto.
        let mut cu = unit.cu.unit.clone();
        let before = cu.argument.clone();
        transform(&mut cu);
        if let (true, Some(raw)) = (cu.argument == before, unit.raw_proto.as_ref()) {
            zout.write_all(raw)?;
        } else {
            let ic = IndexedCompilation { unit: cu };
            zout.write_all(&encode_indexed_compilation(&ic))?;
        }
        let mut copied = 0;
        let mut seen: HashSet<&str> = HashSet::new();
        for fi in &unit.cu.unit.required_input {
            if fi.info.digest.is_empty() { continue; }
            if !seen.insert(fi.info.digest.as_str()) { continue; }
            let entry = format!("root/files/{}", fi.info.digest);
            let mut src_e = match self.zin.by_name(&entry) {
                Ok(e) => e,
                Err(_) => continue, // not in kzip (compiler builtin / external)
            };
            zout.start_file(&entry, opts)?;
            std::io::copy(&mut src_e, &mut zout)?;
            copied += 1;
        }
        zout.finish()?.flush()?;
        Ok(copied)
    }
}

/// One-shot: read every unit from `src`, write a proto-only kzip at
/// `dst`. Returns `(n_units, n_files)`.
pub fn normalize(src: &Path, dst: &Path) -> Result<(usize, usize)> {
    normalize_progress(src, dst, NoProgress)
}

/// Same as [`normalize`] but invokes `progress.report` periodically
/// during read and write phases. Phase strings: `"read"`,
/// `"write-units"`, `"write-files"`.
pub fn normalize_progress<P: Progress>(
    src: &Path, dst: &Path, mut progress: P,
) -> Result<(usize, usize)> {
    let units = read_units_progress(src, &mut progress)?;
    let mut files: HashSet<String> = HashSet::new();
    for u in &units {
        for fi in &u.cu.unit.required_input {
            if !fi.info.digest.is_empty() { files.insert(fi.info.digest.clone()); }
        }
    }
    write_normalized_progress(src, &units, dst, &mut progress)?;
    Ok((units.len(), files.len()))
}

// ----------------------------------------------------------------- proto codec

// ---- decode (parser) ----

fn parse_indexed_compilation(buf: &[u8]) -> Result<IndexedCompilation> {
    let mut out = IndexedCompilation::default();
    let mut pos = 0;
    while pos < buf.len() {
        let (field, wire, len) = read_field_header(buf, &mut pos)?;
        let slice = take(buf, &mut pos, len)?;
        match (field, wire) {
            (1, 2) => out.unit = parse_compilation_unit(slice)?,
            _      => { /* skip — index/file_data ignored */ }
        }
    }
    Ok(out)
}

fn parse_compilation_unit(buf: &[u8]) -> Result<CompilationUnit> {
    let mut out = CompilationUnit::default();
    let mut pos = 0;
    while pos < buf.len() {
        let (field, wire, len) = read_field_header(buf, &mut pos)?;
        let slice = take(buf, &mut pos, len)?;
        // kythe.proto.CompilationUnit field numbers (analysis.proto):
        //   v_name=1, required_input=3, has_compile_errors=4 (bool, skip),
        //   argument=5, source_file=6, output_key=7,
        //   working_directory=8, entry_context=9,
        //   environment=10, details=11 (skip).
        match (field, wire) {
            (1, 2) => out.v_name = parse_vname(slice)?,
            (3, 2) => out.required_input.push(parse_file_input(slice)?),
            (5, 2) => out.argument.push(String::from_utf8_lossy(slice).into_owned()),
            (6, 2) => out.source_file.push(String::from_utf8_lossy(slice).into_owned()),
            (7, 2) => out.output_key = String::from_utf8_lossy(slice).into_owned(),
            (8, 2) => out.working_directory = String::from_utf8_lossy(slice).into_owned(),
            (9, 2) => out.entry_context = String::from_utf8_lossy(slice).into_owned(),
            _      => { /* has_compile_errors, environment, details — skipped */ }
        }
    }
    Ok(out)
}

fn parse_file_input(buf: &[u8]) -> Result<FileInput> {
    let mut out = FileInput::default();
    let mut pos = 0;
    while pos < buf.len() {
        let (field, wire, len) = read_field_header(buf, &mut pos)?;
        let slice = take(buf, &mut pos, len)?;
        match (field, wire) {
            (1, 2) => out.v_name = parse_vname(slice)?,
            (2, 2) => out.info   = parse_file_info(slice)?,
            _      => {}
        }
    }
    Ok(out)
}

fn parse_file_info(buf: &[u8]) -> Result<FileInfo> {
    let mut out = FileInfo::default();
    let mut pos = 0;
    while pos < buf.len() {
        let (field, wire, len) = read_field_header(buf, &mut pos)?;
        let slice = take(buf, &mut pos, len)?;
        match (field, wire) {
            (1, 2) => out.path   = String::from_utf8_lossy(slice).into_owned(),
            (2, 2) => out.digest = String::from_utf8_lossy(slice).into_owned(),
            _      => {}
        }
    }
    Ok(out)
}

fn parse_vname(buf: &[u8]) -> Result<VName> {
    let mut out = VName::default();
    let mut pos = 0;
    while pos < buf.len() {
        let (field, wire, len) = read_field_header(buf, &mut pos)?;
        let slice = take(buf, &mut pos, len)?;
        match (field, wire) {
            (1, 2) => out.signature = String::from_utf8_lossy(slice).into_owned(),
            (2, 2) => out.corpus    = String::from_utf8_lossy(slice).into_owned(),
            (3, 2) => out.root      = String::from_utf8_lossy(slice).into_owned(),
            (4, 2) => out.path      = String::from_utf8_lossy(slice).into_owned(),
            (5, 2) => out.language  = String::from_utf8_lossy(slice).into_owned(),
            _      => {}
        }
    }
    Ok(out)
}

fn read_field_header(buf: &[u8], pos: &mut usize) -> Result<(u32, u8, usize)> {
    let tag = read_varint_bytes(buf, pos)?;
    let field = (tag >> 3) as u32;
    let wire  = (tag & 0x7) as u8;
    if wire != 2 {
        bail!("unexpected wire type {wire} for field {field} at byte {pos}", pos = *pos);
    }
    let len = read_varint_bytes(buf, pos)? as usize;
    Ok((field, wire, len))
}

fn read_varint_bytes(buf: &[u8], pos: &mut usize) -> Result<u64> {
    let mut val: u64 = 0;
    let mut shift = 0u32;
    for _ in 0..10 {
        if *pos >= buf.len() { bail!("truncated varint at byte {}", *pos); }
        let b = buf[*pos];
        *pos += 1;
        val |= ((b & 0x7F) as u64) << shift;
        if b & 0x80 == 0 { return Ok(val); }
        shift += 7;
    }
    Err(anyhow!("varint > 10 bytes"))
}

fn take<'a>(buf: &'a [u8], pos: &mut usize, len: usize) -> Result<&'a [u8]> {
    let end = pos.checked_add(len).ok_or_else(|| anyhow!("len overflow"))?;
    if end > buf.len() { bail!("truncated field at byte {pos}"); }
    let slice = &buf[*pos..end];
    *pos = end;
    Ok(slice)
}

// ---- encode (serializer) ----

fn encode_indexed_compilation(c: &IndexedCompilation) -> Vec<u8> {
    let mut out = Vec::new();
    let mut unit_buf = Vec::new();
    encode_compilation_unit(&c.unit, &mut unit_buf);
    write_tag_len(1, &unit_buf, &mut out);
    out
}

fn encode_compilation_unit(c: &CompilationUnit, out: &mut Vec<u8>) {
    // See decoder for the canonical field numbers.
    let mut v = Vec::new();
    encode_vname(&c.v_name, &mut v);
    if !v.is_empty() { write_tag_len(1, &v, out); }
    for fi in &c.required_input {
        let mut b = Vec::new();
        encode_file_input(fi, &mut b);
        write_tag_len(3, &b, out);
    }
    for a in &c.argument        { write_tag_len(5, a.as_bytes(), out); }
    for s in &c.source_file     { write_tag_len(6, s.as_bytes(), out); }
    if !c.output_key.is_empty() { write_tag_len(7, c.output_key.as_bytes(), out); }
    if !c.working_directory.is_empty() {
        write_tag_len(8, c.working_directory.as_bytes(), out);
    }
    if !c.entry_context.is_empty() {
        write_tag_len(9, c.entry_context.as_bytes(), out);
    }
}

fn encode_file_input(fi: &FileInput, out: &mut Vec<u8>) {
    let mut v = Vec::new(); encode_vname(&fi.v_name, &mut v);
    if !v.is_empty() { write_tag_len(1, &v, out); }
    let mut i = Vec::new(); encode_file_info(&fi.info, &mut i);
    if !i.is_empty() { write_tag_len(2, &i, out); }
}

fn encode_file_info(fi: &FileInfo, out: &mut Vec<u8>) {
    if !fi.path.is_empty()   { write_tag_len(1, fi.path.as_bytes(),   out); }
    if !fi.digest.is_empty() { write_tag_len(2, fi.digest.as_bytes(), out); }
}

fn encode_vname(v: &VName, out: &mut Vec<u8>) {
    if !v.signature.is_empty() { write_tag_len(1, v.signature.as_bytes(), out); }
    if !v.corpus.is_empty()    { write_tag_len(2, v.corpus.as_bytes(),    out); }
    if !v.root.is_empty()      { write_tag_len(3, v.root.as_bytes(),      out); }
    if !v.path.is_empty()      { write_tag_len(4, v.path.as_bytes(),      out); }
    if !v.language.is_empty()  { write_tag_len(5, v.language.as_bytes(),  out); }
}

fn write_tag_len(field: u32, data: &[u8], out: &mut Vec<u8>) {
    write_varint(((field as u64) << 3) | 2, out);
    write_varint(data.len() as u64, out);
    out.extend_from_slice(data);
}

fn write_varint(mut v: u64, out: &mut Vec<u8>) {
    while v >= 0x80 { out.push(((v as u8) & 0x7F) | 0x80); v >>= 7; }
    out.push(v as u8);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn vname_proto_round_trip() {
        let v = VName {
            signature: "Foo()".into(),
            corpus:    "test".into(),
            root:      "r".into(),
            path:      "foo/bar.cpp".into(),
            language:  "c++".into(),
        };
        let mut bytes = Vec::new();
        encode_vname(&v, &mut bytes);
        let parsed = parse_vname(&bytes).unwrap();
        assert_eq!(parsed.signature, v.signature);
        assert_eq!(parsed.corpus, v.corpus);
        assert_eq!(parsed.root, v.root);
        assert_eq!(parsed.path, v.path);
        assert_eq!(parsed.language, v.language);
    }

    #[test]
    fn cu_full_round_trip() {
        let cu = IndexedCompilation {
            unit: CompilationUnit {
                v_name: VName { language: "c++".into(),
                                corpus: "test".into(), ..Default::default() },
                required_input: vec![FileInput {
                    v_name: VName { path: "foo.h".into(), ..Default::default() },
                    info:   FileInfo { path: "foo.h".into(), digest: "abc123".into() },
                }],
                argument:    vec!["clang++".into(), "-c".into(), "foo.cpp".into()],
                source_file: vec!["foo.cpp".into()],
                output_key:  "foo.o".into(),
                working_directory: "/build".into(),
                entry_context: "ctx".into(),
            },
        };
        let bytes = encode_indexed_compilation(&cu);
        let parsed = parse_indexed_compilation(&bytes).unwrap();
        assert_eq!(parsed.unit.v_name.language, "c++");
        assert_eq!(parsed.unit.required_input.len(), 1);
        assert_eq!(parsed.unit.required_input[0].info.digest, "abc123");
        assert_eq!(parsed.unit.argument, vec!["clang++", "-c", "foo.cpp"]);
        assert_eq!(parsed.unit.source_file, vec!["foo.cpp"]);
        assert_eq!(parsed.unit.output_key, "foo.o");
        assert_eq!(parsed.unit.working_directory, "/build");
        assert_eq!(parsed.unit.entry_context, "ctx");
    }

    #[test]
    fn compilation_unit_decodes_real_kythe_wire_bytes() {
        // Hand-built wire bytes matching kythe.proto.CompilationUnit
        // field numbers exactly (per analysis.proto v0.0.75). If our
        // encoder/decoder ever drift from the spec, this fails.
        // Layout:
        //   field 1 (v_name)         — submessage, tag=0x0a
        //     field 5 (language)     — "c++"      tag=0x2a
        //   field 5 (argument)       — "clang++"  tag=0x2a
        //   field 6 (source_file)    — "foo.cpp"  tag=0x32
        //   field 7 (output_key)     — "foo.o"    tag=0x3a
        //   field 8 (working_dir)    — "/b"       tag=0x42
        //   field 9 (entry_context)  — "ctx"      tag=0x4a
        let mut wire = Vec::new();
        // v_name { language: "c++" }
        wire.extend_from_slice(&[0x0a, 5,  0x2a, 3, b'c', b'+', b'+']);
        // argument: "clang++"
        wire.extend_from_slice(&[0x2a, 7,  b'c', b'l', b'a', b'n', b'g', b'+', b'+']);
        // source_file: "foo.cpp"
        wire.extend_from_slice(&[0x32, 7,  b'f', b'o', b'o', b'.', b'c', b'p', b'p']);
        // output_key: "foo.o"
        wire.extend_from_slice(&[0x3a, 5,  b'f', b'o', b'o', b'.', b'o']);
        // working_directory: "/b"
        wire.extend_from_slice(&[0x42, 2,  b'/', b'b']);
        // entry_context: "ctx"
        wire.extend_from_slice(&[0x4a, 3,  b'c', b't', b'x']);

        let cu = parse_compilation_unit(&wire).unwrap();
        assert_eq!(cu.v_name.language, "c++");
        assert_eq!(cu.argument, vec!["clang++"]);
        assert_eq!(cu.source_file, vec!["foo.cpp"]);
        assert_eq!(cu.output_key, "foo.o");
        assert_eq!(cu.working_directory, "/b");
        assert_eq!(cu.entry_context, "ctx");

        // And our encoder emits the same canonical bytes.
        let mut re = Vec::new();
        encode_compilation_unit(&cu, &mut re);
        assert_eq!(re, wire, "encoder output drifted from canonical wire bytes");
    }

    #[test]
    fn strip_prefix_rejects_directory_entries() {
        // The kzip spec mandates directory entries `root/units/` and
        // `root/pbunits/`. They must not be parsed as units.
        assert_eq!(strip_prefix("root/units/", "root/units/"), None);
        assert_eq!(strip_prefix("root/pbunits/", "root/pbunits/"), None);
        // Real entries still pass.
        assert_eq!(strip_prefix("root/units/abc123", "root/units/"), Some("abc123"));
        // Nested paths still rejected.
        assert_eq!(strip_prefix("root/units/sub/abc", "root/units/"), None);
    }

    /// End-to-end: build a tiny mixed-encoding kzip in memory, run
    /// `normalize_progress` against it, and verify the progress
    /// callback fires for every phase. Locks the contract that
    /// long-running ops emit observable progress instead of going
    /// silent for minutes at a time.
    #[test]
    fn sub_kzip_writer_extracts_one_cu_with_files() {
        // Build a kzip with two units (each with its own file blob);
        // extract one with SubKzipWriter and assert the output kzip
        // has exactly that unit + its blob, not the other one.
        use std::io::Write;
        let dir = std::env::temp_dir().join(format!("scry2-subkzip-{}", std::process::id()));
        let _ = std::fs::create_dir_all(&dir);
        let src = dir.join("in.kzip");
        let dst = dir.join("out.kzip");

        let f = std::fs::File::create(&src).unwrap();
        let mut z = zip::ZipWriter::new(f);
        let opts = zip::write::FileOptions::default()
            .compression_method(zip::CompressionMethod::Stored);
        z.add_directory("root/", opts).unwrap();
        z.add_directory("root/pbunits/", opts).unwrap();
        z.add_directory("root/files/", opts).unwrap();
        for (sha, digest) in [("aaa", "111"), ("bbb", "222")] {
            let cu = IndexedCompilation {
                unit: CompilationUnit {
                    v_name: VName { language: "c++".into(), ..Default::default() },
                    required_input: vec![FileInput {
                        v_name: VName::default(),
                        info: FileInfo { path: format!("f-{sha}.h"), digest: digest.into() },
                    }],
                    ..Default::default()
                },
            };
            z.start_file(format!("root/pbunits/{sha}"), opts).unwrap();
            z.write_all(&encode_indexed_compilation(&cu)).unwrap();
            z.start_file(format!("root/files/{digest}"), opts).unwrap();
            z.write_all(format!("contents-of-{digest}").as_bytes()).unwrap();
        }
        z.finish().unwrap();

        // Re-read the source kzip via our own walker so we get a
        // round-tripped Unit (matching production flow).
        let units = read_units(&src).unwrap();
        assert_eq!(units.len(), 2);
        let unit_aaa = units.iter().find(|u| u.sha == "aaa").unwrap();

        let mut w = SubKzipWriter::open(&src).unwrap();
        let copied = w.extract(unit_aaa, &dst).unwrap();
        assert_eq!(copied, 1, "one file blob for unit aaa");

        // Open the extracted kzip and verify content.
        let extracted = std::fs::File::open(&dst).unwrap();
        let mut z2 = zip::ZipArchive::new(extracted).unwrap();
        let names: std::collections::HashSet<String> =
            (0..z2.len()).map(|i| z2.by_index(i).unwrap().name().to_string()).collect();
        assert!(names.contains("root/pbunits/aaa"));
        assert!(names.contains("root/files/111"));
        assert!(!names.contains("root/pbunits/bbb"), "unit bbb must not leak");
        assert!(!names.contains("root/files/222"), "blob for bbb must not leak");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn primary_path_prefers_source_file() {
        let cu = IndexedCompilation {
            unit: CompilationUnit {
                source_file: vec!["foo.cpp".into()],
                required_input: vec![FileInput {
                    v_name: VName { path: "bar.h".into(), ..Default::default() },
                    info: FileInfo { path: "bar.h".into(), digest: "abc".into() },
                }],
                ..Default::default()
            },
        };
        let u = Unit { sha: "x".into(), cu, raw_proto: None };
        assert_eq!(u.primary_path(), Some("foo.cpp"));
    }

    #[test]
    fn primary_path_falls_back_to_first_required_input() {
        let cu = IndexedCompilation {
            unit: CompilationUnit {
                source_file: vec![],
                required_input: vec![FileInput {
                    v_name: VName { path: "bar.h".into(), ..Default::default() },
                    info: FileInfo { path: "bar.h".into(), digest: "abc".into() },
                }],
                ..Default::default()
            },
        };
        let u = Unit { sha: "x".into(), cu, raw_proto: None };
        assert_eq!(u.primary_path(), Some("bar.h"));
    }

    #[test]
    fn language_normalizes_to_lowercase() {
        let cu = IndexedCompilation {
            unit: CompilationUnit {
                v_name: VName { language: "JAVA".into(), ..Default::default() },
                ..Default::default()
            },
        };
        let u = Unit { sha: "x".into(), cu, raw_proto: None };
        assert_eq!(u.language(), "java");
    }

    #[test]
    fn normalize_progress_invokes_callback_for_every_phase() {
        use std::io::Write;
        let dir = std::env::temp_dir().join(format!("scry2-kzip-prog-{}", std::process::id()));
        let _ = std::fs::create_dir_all(&dir);
        let src = dir.join("in.kzip");
        let dst = dir.join("out.kzip");

        // Build a kzip with one proto unit, one JSON unit, and one
        // file blob. Exercises both reader passes + writer.
        let f = std::fs::File::create(&src).unwrap();
        let mut z = zip::ZipWriter::new(f);
        let opts = zip::write::FileOptions::default()
            .compression_method(zip::CompressionMethod::Stored);
        z.add_directory("root/", opts).unwrap();
        z.add_directory("root/pbunits/", opts).unwrap();
        z.add_directory("root/units/", opts).unwrap();
        z.add_directory("root/files/", opts).unwrap();
        // One proto unit pointing at digest "abc".
        let cu = IndexedCompilation {
            unit: CompilationUnit {
                v_name: VName { language: "c++".into(), ..Default::default() },
                required_input: vec![FileInput {
                    v_name: VName::default(),
                    info: FileInfo { path: "foo.h".into(), digest: "abc".into() },
                }],
                ..Default::default()
            },
        };
        z.start_file("root/pbunits/aaa", opts).unwrap();
        z.write_all(&encode_indexed_compilation(&cu)).unwrap();
        // One JSON unit pointing at a different digest.
        z.start_file("root/units/bbb", opts).unwrap();
        z.write_all(br#"{"unit":{"vName":{"language":"java"},"requiredInput":[{"info":{"path":"X.java","digest":"def"}}]}}"#).unwrap();
        // File blobs.
        z.start_file("root/files/abc", opts).unwrap();
        z.write_all(b"void foo();").unwrap();
        z.start_file("root/files/def", opts).unwrap();
        z.write_all(b"class X {}").unwrap();
        z.finish().unwrap();

        #[derive(Default)]
        struct Recorder {
            calls: std::collections::HashMap<String, (usize, usize)>,
        }
        impl Progress for Recorder {
            fn report(&mut self, phase: &str, done: usize, total: usize) {
                let e = self.calls.entry(phase.to_string()).or_insert((0, 0));
                e.0 += 1;
                e.1 = e.1.max(done);
                assert!(done <= total.max(done), "done={done} total={total}");
            }
        }
        let mut rec = Recorder::default();
        let (n_units, n_files) = normalize_progress(&src, &dst, &mut rec).unwrap();
        assert_eq!(n_units, 2, "1 proto + 1 JSON");
        assert_eq!(n_files, 2, "2 file blobs");
        // Each phase must have been reported at least once, and the
        // last `done` for each must equal the total work.
        for phase in &["read", "write-units", "write-files"] {
            let (calls, max_done) = rec.calls.get(*phase)
                .unwrap_or_else(|| panic!("phase {phase} never reported"));
            assert!(*calls >= 1, "phase {phase}: at least one progress call");
            assert_eq!(*max_done, 2, "phase {phase}: last done == total work");
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn json_unit_decodes_with_aliases() {
        // AOSP extractors emit snake_case fields. Confirm aliases work
        // for camelCase too per the proto3-JSON spec.
        let snake = r#"{"unit":{"v_name":{"language":"java"},
                                "required_input":[{"v_name":{"path":"X.java"},
                                                   "info":{"path":"X.java","digest":"abc"}}],
                                "argument":["javac","-d","out"]}}"#;
        let camel = r#"{"unit":{"vName":{"language":"java"},
                                "requiredInput":[{"vName":{"path":"X.java"},
                                                  "info":{"path":"X.java","digest":"abc"}}],
                                "argument":["javac","-d","out"]}}"#;
        for src in [snake, camel] {
            let c: IndexedCompilation = serde_json::from_str(src).unwrap();
            assert_eq!(c.unit.v_name.language, "java");
            assert_eq!(c.unit.required_input.len(), 1);
            assert_eq!(c.unit.required_input[0].info.digest, "abc");
        }
    }
}
