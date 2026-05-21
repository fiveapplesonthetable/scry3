//! Query verbs — thin wrappers over the stock `kythe` CLI against a
//! LevelDB serving table, with name → ticket resolution layered on top via
//! the `scry3` name index.
//!
//! Heavy data (definitions, references, callers, edges, node facts) all
//! comes straight from `kythe`; scry3 only resolves the human name to a
//! ticket and reshapes the JSON into terse `path:line:col` lines. `--json`
//! passes the kythe reply through untouched. The verb surface deliberately
//! mirrors scry2 (def / ref / callers / super / sub / stat / names / repl)
//! so the two tools feel the same at the command line.

use anyhow::{bail, Context, Result};
use std::cell::RefCell;
use std::io::{BufRead, Write};
use std::path::{Path, PathBuf};
use std::process::Command;

use crate::http::HttpClient;
use crate::nameindex::NameIndex;

/// Path-substring filters shared by the anchor-returning verbs, matching
/// scry2's `--in` / `--not-in` semantics (empty string is a no-op).
#[derive(Default, Clone)]
pub struct Filter {
    pub in_: Option<String>,
    pub not_in: Option<String>,
    /// Keep only symbols whose *definition* file path contains this. Applied
    /// at name→ticket resolution (scry2's `--def-in`).
    pub def_in: Option<String>,
}

impl Filter {
    fn keep(&self, path: &str) -> bool {
        if let Some(s) = &self.in_ {
            if !s.is_empty() && !path.contains(s.as_str()) {
                return false;
            }
        }
        if let Some(s) = &self.not_in {
            if !s.is_empty() && path.contains(s.as_str()) {
                return false;
            }
        }
        true
    }
}

pub struct Ctx {
    pub kythe_bin: PathBuf,
    pub serving: PathBuf,
    /// Loaded once (in `build_ctx` or at `repl` start) so a long-lived
    /// session never re-reads the index — the main warm-path win over a
    /// fresh process per query.
    pub names: Option<NameIndex>,
    /// When set, queries hit a warm `http_server` over a kept-alive socket
    /// instead of spawning a fresh `kythe` process per call — the fast path.
    pub http: Option<RefCell<HttpClient>>,
    pub json: bool,
    pub limit: usize,
}

impl Ctx {
    fn kythe(&self) -> Command {
        let mut c = Command::new(&self.kythe_bin);
        c.arg("--json").arg("--api").arg(&self.serving);
        c
    }

    fn http_post(&self, path: &str, body: &str) -> Option<Result<serde_json::Value>> {
        let cell = self.http.as_ref()?;
        let mut client = cell.borrow_mut();
        Some(match client.post(path, body) {
            Ok(bytes) if bytes.is_empty() => Ok(serde_json::Value::Null),
            Ok(bytes) => serde_json::from_slice(&bytes).context("parse server json"),
            Err(e) => Err(e),
        })
    }
}

fn cli_def_kind(v: &str) -> &'static str {
    match v {
        "all" => "ALL_DEFINITIONS",
        "binding" => "BINDING_DEFINITIONS",
        _ => "NO_DEFINITIONS",
    }
}
fn cli_ref_kind(v: &str) -> &'static str {
    match v {
        "all" => "ALL_REFERENCES",
        "call" => "CALL_REFERENCES",
        "noncall" => "NON_CALL_REFERENCES",
        _ => "NO_REFERENCES",
    }
}
fn cli_caller_kind(v: &str) -> &'static str {
    match v {
        "direct" => "DIRECT_CALLERS",
        "overrides" => "OVERRIDE_CALLERS",
        _ => "NO_CALLERS",
    }
}

fn run_json(mut cmd: Command) -> Result<serde_json::Value> {
    let out = cmd.output().context("spawn kythe")?;
    if !out.status.success() {
        bail!("kythe failed: {}", String::from_utf8_lossy(&out.stderr).trim());
    }
    if out.stdout.is_empty() {
        return Ok(serde_json::Value::Null);
    }
    serde_json::from_slice(&out.stdout).context("parse kythe json")
}

fn run_passthrough(mut cmd: Command) -> Result<()> {
    let st = cmd.status().context("spawn kythe")?;
    if !st.success() {
        bail!("kythe exited with {st}");
    }
    Ok(())
}

fn is_ticket(s: &str) -> bool {
    s.starts_with("kythe:")
}

/// File path of `ticket`'s definition (for `--def-in`).
fn def_path(ctx: &Ctx, ticket: &str) -> Option<String> {
    let reply = xrefs_for(ctx, ticket, "all", "none", "none").ok()?;
    let crs = reply.get("cross_references")?.as_object()?;
    for set in crs.values() {
        for ra in set.get("definition").and_then(|v| v.as_array()).into_iter().flatten() {
            if let Some(anchor) = ra.get("anchor") {
                let p = anchor_path(anchor);
                if !p.is_empty() {
                    return Some(p);
                }
            }
        }
    }
    None
}

fn resolve(ctx: &Ctx, name: &str, substr: bool, filter: &Filter, cap: usize) -> Result<Vec<String>> {
    if is_ticket(name) {
        return Ok(vec![name.to_string()]);
    }
    let idx = ctx.names.as_ref().context(
        "name resolution needs a name index (build one with `scry3 name-index`, \
         default location <serving>/scry3.names.idx)",
    )?;
    let mut tickets: Vec<String> = if substr {
        idx.substr(name, ctx.limit).into_iter().map(|(_, t)| t).collect()
    } else {
        idx.exact(name)
    };
    if let Some(s) = filter.def_in.as_deref().filter(|s| !s.is_empty()) {
        tickets.retain(|t| def_path(ctx, t).map(|p| p.contains(s)).unwrap_or(false));
    }
    if tickets.is_empty() {
        bail!("no ticket for name {name:?} (try --substr / check --def-in)");
    }
    if cap > 0 && tickets.len() > cap {
        tickets.truncate(cap);
    }
    Ok(tickets)
}

/// Pull the `path=` query parameter out of a ticket, percent-decoded.
fn path_of(ticket: &str) -> String {
    if let Some(i) = ticket.find("?path=") {
        let rest = &ticket[i + 6..];
        let end = rest.find(['?', '#']).unwrap_or(rest.len());
        return percent_decode(&rest[..end]);
    }
    ticket.to_string()
}

fn sig_of(ticket: &str) -> String {
    ticket
        .rsplit_once('#')
        .map(|(_, s)| percent_decode(s))
        .unwrap_or_default()
}

/// Resolve a ticket to a human name via the reverse name index, falling back
/// to the signature.
fn label(ctx: &Ctx, ticket: &str) -> String {
    if let Some(idx) = &ctx.names {
        if let Some(n) = idx.name_of(ticket) {
            return n.to_string();
        }
    }
    sig_of(ticket)
}

fn percent_decode(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%' && i + 2 < b.len() {
            if let (Some(h), Some(l)) =
                ((b[i + 1] as char).to_digit(16), (b[i + 2] as char).to_digit(16))
            {
                out.push((h * 16 + l) as u8);
                i += 3;
                continue;
            }
        }
        out.push(b[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn anchor_path(anchor: &serde_json::Value) -> String {
    let parent = anchor.get("parent").and_then(|v| v.as_str()).unwrap_or("");
    if parent.is_empty() {
        anchor.get("ticket").and_then(|v| v.as_str()).map(path_of).unwrap_or_default()
    } else {
        path_of(parent)
    }
}

fn anchor_line(anchor: &serde_json::Value, path: &str) -> String {
    let start = anchor.get("span").and_then(|s| s.get("start"));
    let line = start.and_then(|s| s.get("line_number")).and_then(|v| v.as_u64()).unwrap_or(0);
    let col = start.and_then(|s| s.get("column_offset")).and_then(|v| v.as_u64()).unwrap_or(0);
    let snippet = anchor
        .get("snippet")
        .and_then(|v| v.as_str())
        .or_else(|| anchor.get("text").and_then(|v| v.as_str()))
        .unwrap_or("")
        .trim();
    format!("{path}:{line}:{col}  {snippet}")
}

fn print_anchors(
    reply: &serde_json::Value,
    key: &str,
    header: &str,
    filter: &Filter,
    limit: usize,
) -> usize {
    let mut n = 0;
    if let Some(crs) = reply.get("cross_references").and_then(|v| v.as_object()) {
        for set in crs.values() {
            if let Some(arr) = set.get(key).and_then(|v| v.as_array()) {
                for item in arr {
                    if let Some(anchor) = item.get("anchor") {
                        let path = anchor_path(anchor);
                        if !filter.keep(&path) {
                            continue;
                        }
                        if n == 0 {
                            println!("{header}");
                        }
                        println!("  {}", anchor_line(anchor, &path));
                        n += 1;
                        if n >= limit {
                            return n;
                        }
                    }
                }
            }
        }
    }
    n
}

fn xrefs_for(ctx: &Ctx, ticket: &str, defs: &str, refs: &str, callers: &str) -> Result<serde_json::Value> {
    // Fast path: POST to the warm server (no kythe spawn).
    let body = serde_json::json!({
        "ticket": [ticket],
        "definition_kind": cli_def_kind(defs),
        "reference_kind": cli_ref_kind(refs),
        "caller_kind": cli_caller_kind(callers),
        "declaration_kind": "NO_DECLARATIONS",
        "snippets": "DEFAULT",
    })
    .to_string();
    if let Some(r) = ctx.http_post("xrefs", &body) {
        return r;
    }
    // Expensive path: spawn the kythe CLI.
    let mut c = ctx.kythe();
    c.arg("xrefs")
        .arg("--definitions").arg(defs)
        .arg("--references").arg(refs)
        .arg("--callers").arg(callers)
        .arg("--declarations").arg("none")
        .arg("--related_nodes=false")
        .arg(ticket);
    run_json(c)
}

fn edges_for(ctx: &Ctx, ticket: &str, kinds: &[&str]) -> Result<serde_json::Value> {
    let body = serde_json::json!({ "ticket": [ticket], "kind": kinds }).to_string();
    if let Some(r) = ctx.http_post("edges", &body) {
        return r;
    }
    let mut c = ctx.kythe();
    c.arg("edges").arg("--kinds").arg(kinds.join(",")).arg("--targets_only").arg(ticket);
    run_json(c)
}

pub fn def(ctx: &Ctx, name: &str, substr: bool, filter: &Filter) -> Result<()> {
    let tickets = resolve(ctx, name, substr, filter, ctx.limit)?;
    let mut total = 0;
    for t in &tickets {
        let reply = xrefs_for(ctx, t, "all", "none", "none")?;
        if ctx.json {
            println!("{}", serde_json::to_string(&reply)?);
        } else {
            total += print_anchors(&reply, "definition", &format!("def {name} [{}]", label(ctx, t)), filter, ctx.limit);
        }
    }
    if !ctx.json && total == 0 {
        println!("(no definitions)");
    }
    Ok(())
}

pub fn references(ctx: &Ctx, name: &str, substr: bool, filter: &Filter) -> Result<()> {
    let tickets = resolve(ctx, name, substr, filter, ctx.limit)?;
    let mut total = 0;
    for t in &tickets {
        let reply = xrefs_for(ctx, t, "none", "all", "none")?;
        if ctx.json {
            println!("{}", serde_json::to_string(&reply)?);
        } else {
            total += print_anchors(&reply, "reference", &format!("ref {name} [{}]", label(ctx, t)), filter, ctx.limit);
        }
    }
    if !ctx.json && total == 0 {
        println!("(no references)");
    }
    Ok(())
}

pub fn callers(ctx: &Ctx, name: &str, substr: bool, filter: &Filter) -> Result<()> {
    let tickets = resolve(ctx, name, substr, filter, ctx.limit)?;
    let mut total = 0;
    for t in &tickets {
        let reply = xrefs_for(ctx, t, "none", "call", "direct")?;
        if ctx.json {
            println!("{}", serde_json::to_string(&reply)?);
        } else {
            total += print_anchors(&reply, "caller", &format!("callers {name} [{}]", label(ctx, t)), filter, ctx.limit);
            total += print_anchors(&reply, "reference", "  (call sites)", filter, ctx.limit);
        }
    }
    if !ctx.json && total == 0 {
        println!("(no callers)");
    }
    Ok(())
}

const INHERIT_FWD: &[&str] = &[
    "/kythe/edge/extends",
    "/kythe/edge/extends/public",
    "/kythe/edge/extends/protected",
    "/kythe/edge/extends/private",
    "/kythe/edge/overrides",
    "/kythe/edge/satisfies",
];

fn inherit_kinds(sub: bool) -> Vec<String> {
    if sub {
        INHERIT_FWD.iter().map(|k| format!("%{k}")).collect()
    } else {
        INHERIT_FWD.iter().map(|k| k.to_string()).collect()
    }
}

/// Deduped target tickets of `ticket` over `kinds`.
fn edge_targets(ctx: &Ctx, ticket: &str, kinds: &[&str]) -> Result<Vec<String>> {
    let reply = edges_for(ctx, ticket, kinds)?;
    let mut seen = std::collections::HashSet::new();
    let mut out = Vec::new();
    if let Some(sets) = reply.get("edge_sets").and_then(|v| v.as_object()) {
        for set in sets.values() {
            if let Some(groups) = set.get("groups").and_then(|v| v.as_object()) {
                for grp in groups.values() {
                    if let Some(edges) = grp.get("edge").and_then(|v| v.as_array()) {
                        for e in edges {
                            if let Some(tt) = e.get("target_ticket").and_then(|v| v.as_str()) {
                                if seen.insert(tt.to_string()) {
                                    out.push(tt.to_string());
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    Ok(out)
}

/// `super` / `sub` — supertypes / subtypes via the inheritance edges.
/// `super` follows the forward edges; `sub` follows their reverse (`%`).
pub fn inheritance(ctx: &Ctx, name: &str, substr: bool, sub: bool, filter: &Filter) -> Result<()> {
    let owned = inherit_kinds(sub);
    let kinds: Vec<&str> = owned.iter().map(|s| s.as_str()).collect();
    let tickets = resolve(ctx, name, substr, filter, ctx.limit)?;
    let verb = if sub { "sub" } else { "super" };
    let mut total = 0;
    for t in &tickets {
        let targets = edge_targets(ctx, t, &kinds)?;
        if ctx.json {
            println!("{}", serde_json::json!({"name": name, "ticket": t, "targets": targets}));
            total += targets.len();
            continue;
        }
        for tt in targets {
            let p = path_of(&tt);
            if !filter.keep(&p) {
                continue;
            }
            if total == 0 {
                println!("{verb} {name} [{}]", label(ctx, t));
            }
            println!("  {}  [{}]", label(ctx, &tt), p);
            total += 1;
            if total >= ctx.limit {
                break;
            }
        }
    }
    if !ctx.json && total == 0 {
        println!("(no {verb}types)");
    }
    Ok(())
}

/// Semantic callers of `ticket` (functions whose bodies call it).
fn callers_of(ctx: &Ctx, ticket: &str) -> Result<Vec<String>> {
    let reply = xrefs_for(ctx, ticket, "none", "none", "direct")?;
    let mut seen = std::collections::HashSet::new();
    let mut out = Vec::new();
    if let Some(crs) = reply.get("cross_references").and_then(|v| v.as_object()) {
        for set in crs.values() {
            if let Some(arr) = set.get("caller").and_then(|v| v.as_array()) {
                for ra in arr {
                    if let Some(tk) = ra.get("ticket").and_then(|v| v.as_str()) {
                        if seen.insert(tk.to_string()) {
                            out.push(tk.to_string());
                        }
                    }
                }
            }
        }
    }
    Ok(out)
}

/// Decorations (references) of a file ticket — via the warm server's
/// /decorations endpoint, or the `kythe decor` CLI. Returns
/// (target_ticket, kind, start_byte) tuples.
fn decor_refs(ctx: &Ctx, file_ticket: &str) -> Result<Vec<(String, String, u64)>> {
    let reply = if ctx.http.is_some() {
        let body = serde_json::json!({"location": {"ticket": file_ticket}, "references": true})
            .to_string();
        ctx.http_post("decorations", &body).unwrap()?
    } else {
        let mut c = ctx.kythe();
        c.arg("decor").arg(file_ticket);
        run_json(c)?
    };
    let mut out = Vec::new();
    if let Some(refs) = reply.get("reference").and_then(|v| v.as_array()) {
        for r in refs {
            let tt = r.get("target_ticket").and_then(|v| v.as_str()).unwrap_or("");
            let kind = r.get("kind").and_then(|v| v.as_str()).unwrap_or("");
            let start = r
                .get("span").and_then(|s| s.get("start")).and_then(|s| s.get("byte_offset"))
                .and_then(|v| v.as_u64()).unwrap_or(0);
            if !tt.is_empty() {
                out.push((tt.to_string(), kind.to_string(), start));
            }
        }
    }
    Ok(out)
}

/// Callees of `ticket`: decorate its definition body span and collect
/// /kythe/edge/ref/call targets within it.
fn callees_of(ctx: &Ctx, ticket: &str) -> Result<Vec<String>> {
    let def = xrefs_for(ctx, ticket, "all", "none", "none")?;
    let mut seen = std::collections::HashSet::new();
    let mut out = Vec::new();
    if let Some(crs) = def.get("cross_references").and_then(|v| v.as_object()) {
        for set in crs.values() {
            for ra in set.get("definition").and_then(|v| v.as_array()).into_iter().flatten() {
                let anchor = match ra.get("anchor") {
                    Some(a) => a,
                    None => continue,
                };
                let file = anchor.get("parent").and_then(|v| v.as_str()).unwrap_or("");
                let span = anchor.get("span");
                let ds = span.and_then(|s| s.get("start")).and_then(|s| s.get("byte_offset")).and_then(|v| v.as_u64());
                let de = span.and_then(|s| s.get("end")).and_then(|s| s.get("byte_offset")).and_then(|v| v.as_u64());
                if file.is_empty() {
                    continue;
                }
                let (ds, de) = match (ds, de) {
                    (Some(a), Some(b)) => (a, b),
                    _ => (0, u64::MAX),
                };
                for (tt, kind, start) in decor_refs(ctx, file)? {
                    if kind.contains("ref/call") && start >= ds && start < de && seen.insert(tt.clone()) {
                        out.push(tt);
                    }
                }
            }
        }
    }
    Ok(out)
}

fn cg_next(ctx: &Ctx, ticket: &str, dir: &str) -> Result<Vec<String>> {
    Ok(match dir {
        "up" => callers_of(ctx, ticket)?,
        "down" => callees_of(ctx, ticket)?,
        _ => {
            let mut v = callers_of(ctx, ticket)?;
            v.extend(callees_of(ctx, ticket)?);
            v
        }
    })
}

/// `callgraph NAME --direction up|down|both --depth N` — BFS forest like
/// scry2: each node has an id and a parent id (roots have parent -1); every
/// symbol appears once (first discoverer is its parent), which is cycle-safe.
#[allow(clippy::too_many_arguments)]
pub fn callgraph(ctx: &Ctx, name: &str, substr: bool, direction: &str, depth: usize,
                 max_syms: usize, root_limit: usize, filter: &Filter) -> Result<()> {
    if !matches!(direction, "up" | "down" | "both") {
        bail!("--direction must be up|down|both");
    }
    // node = (ticket, parent_id, depth)
    let mut nodes: Vec<(String, i64, usize)> = Vec::new();
    let mut id: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
    for t in resolve(ctx, name, substr, filter, root_limit)? {
        if !id.contains_key(&t) {
            id.insert(t.clone(), nodes.len());
            nodes.push((t, -1, 0));
        }
    }
    let mut qi = 0;
    while qi < nodes.len() && nodes.len() < max_syms {
        let (ticket, _, ndepth) = nodes[qi].clone();
        if ndepth < depth {
            for nx in cg_next(ctx, &ticket, direction)? {
                if !filter.keep(&path_of(&nx)) || id.contains_key(&nx) {
                    continue;
                }
                id.insert(nx.clone(), nodes.len());
                nodes.push((nx, qi as i64, ndepth + 1));
                if nodes.len() >= max_syms {
                    break;
                }
            }
        }
        qi += 1;
    }
    if ctx.json {
        let arr: Vec<_> = nodes.iter().enumerate().map(|(i, (ticket, parent, ndepth))| {
            serde_json::json!({"id": i, "parent": parent, "depth": ndepth,
                "name": label(ctx, ticket), "ticket": ticket, "path": path_of(ticket)})
        }).collect();
        println!("{}", serde_json::json!({"name": name, "direction": direction, "depth": depth, "nodes": arr}));
        return Ok(());
    }
    println!("callgraph {name} ({direction}, depth {depth}) — {} nodes", nodes.len());
    for (i, (ticket, parent, ndepth)) in nodes.iter().enumerate() {
        let p = if *parent < 0 { "-".to_string() } else { parent.to_string() };
        println!("[{i}] parent={p} depth={ndepth}  {}  [{}]", label(ctx, ticket), path_of(ticket));
    }
    Ok(())
}

pub fn identifier(ctx: &Ctx, name: &str, substr: bool) -> Result<()> {
    let idx = ctx.names.as_ref().context("needs a name index")?;
    if substr {
        for (n, t) in idx.substr(name, ctx.limit) {
            if ctx.json {
                println!("{}", serde_json::json!({"name": n, "ticket": t}));
            } else {
                println!("{n}\t{t}");
            }
        }
    } else {
        for t in idx.exact(name) {
            if ctx.json {
                println!("{}", serde_json::json!({"name": name, "ticket": t}));
            } else {
                println!("{t}");
            }
        }
    }
    Ok(())
}

pub fn edges(ctx: &Ctx, target: &str, kinds: Option<&str>) -> Result<()> {
    for t in resolve(ctx, target, false, &Filter::default(), 0)? {
        let mut c = ctx.kythe();
        c.arg("edges");
        if let Some(k) = kinds {
            c.arg("--kinds").arg(k);
        }
        c.arg(&t);
        run_passthrough(c)?;
    }
    Ok(())
}

pub fn nodes(ctx: &Ctx, target: &str) -> Result<()> {
    for t in resolve(ctx, target, false, &Filter::default(), 0)? {
        let mut c = ctx.kythe();
        c.arg("nodes").arg(&t);
        run_passthrough(c)?;
    }
    Ok(())
}

pub fn decor(ctx: &Ctx, file: &str, corpus: Option<&str>) -> Result<()> {
    let mut c = ctx.kythe();
    c.arg("decor");
    if let Some(cp) = corpus {
        if !is_ticket(file) {
            c.arg("--corpus").arg(cp);
        }
    }
    c.arg(file);
    run_passthrough(c)
}

pub fn ls(ctx: &Ctx, path: Option<&str>) -> Result<()> {
    let mut c = ctx.kythe();
    c.arg("ls").arg("--uris");
    if let Some(p) = path {
        c.arg(p);
    }
    run_passthrough(c)
}

/// `stat` — quick health/size readout of the serving table + name index.
pub fn stat(ctx: &Ctx) -> Result<()> {
    let n_files = std::fs::read_dir(&ctx.serving)
        .map(|rd| rd.filter_map(|e| e.ok()).filter(|e| {
            e.path().extension().map(|x| x == "ldb" || x == "sst").unwrap_or(false)
        }).count())
        .unwrap_or(0);
    let bytes: u64 = std::fs::read_dir(&ctx.serving)
        .map(|rd| rd.filter_map(|e| e.ok()).filter_map(|e| e.metadata().ok().map(|m| m.len())).sum())
        .unwrap_or(0);
    let n_rows = ctx.names.as_ref().map(|i| i.len()).unwrap_or(0);
    if ctx.json {
        println!("{}", serde_json::json!({
            "serving": ctx.serving.display().to_string(),
            "leveldb_files": n_files,
            "size_mb": (bytes as f64 / 1e6),
            "name_index_rows": n_rows,
        }));
        return Ok(());
    }
    println!("serving table: {}", ctx.serving.display());
    println!("  leveldb files : {n_files}");
    println!("  size on disk  : {:.1} MB", bytes as f64 / 1e6);
    match &ctx.names {
        Some(_) => println!("  name index    : {} (name,ticket) rows", n_rows),
        None => println!("  name index    : (none — build with `scry3 name-index`)"),
    }
    Ok(())
}

/// `repl` — stdin/stdout loop. The name index is already loaded into `ctx`,
/// so each line skips the ~60 ms index reload a fresh process pays; only the
/// `kythe` subprocess cost remains. Mirrors `scry2 repl` ergonomically.
/// Lines: `<verb> <name> [--substr] [--in S] [--not-in S]`.
pub fn repl(ctx: &Ctx) -> Result<()> {
    let stdin = std::io::stdin();
    let mut stdout = std::io::stdout();
    eprintln!("[repl] ready (verbs: def ref callers super sub callgraph identifier edges nodes; ^D to exit)");
    for line in stdin.lock().lines() {
        let line = line?;
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let toks: Vec<&str> = line.split_whitespace().collect();
        let verb = toks[0];
        let substr = toks.iter().any(|t| *t == "--substr");
        let mut filter = Filter::default();
        let mut name: Option<String> = None;
        let mut direction = "up".to_string();
        let mut depth = 3usize;
        let mut i = 1;
        while i < toks.len() {
            match toks[i] {
                "--substr" => {}
                "--in" => { i += 1; filter.in_ = toks.get(i).map(|s| s.to_string()); }
                "--not-in" => { i += 1; filter.not_in = toks.get(i).map(|s| s.to_string()); }
                "--def-in" => { i += 1; filter.def_in = toks.get(i).map(|s| s.to_string()); }
                "--direction" => { i += 1; if let Some(d) = toks.get(i) { direction = d.to_string(); } }
                "--depth" => { i += 1; if let Some(d) = toks.get(i) { depth = d.parse().unwrap_or(3); } }
                other if name.is_none() => name = Some(other.to_string()),
                _ => {}
            }
            i += 1;
        }
        let Some(name) = name else {
            eprintln!("[repl] usage: <verb> <name> [--substr --in S --not-in S --direction up|down|both --depth N]");
            continue;
        };
        let r = match verb {
            "def" => def(ctx, &name, substr, &filter),
            "ref" => references(ctx, &name, substr, &filter),
            "callers" => callers(ctx, &name, substr, &filter),
            "super" => inheritance(ctx, &name, substr, false, &filter),
            "sub" => inheritance(ctx, &name, substr, true, &filter),
            "callgraph" => callgraph(ctx, &name, substr, &direction, depth, 200, 16, &filter),
            "identifier" | "names" => identifier(ctx, &name, substr || verb == "names"),
            "edges" => edges(ctx, &name, None),
            "nodes" => nodes(ctx, &name),
            other => { eprintln!("[repl] unknown verb {other:?}"); Ok(()) }
        };
        if let Err(e) = r {
            eprintln!("[repl] error: {e:#}");
        }
        let _ = stdout.flush();
    }
    Ok(())
}

pub fn default_kythe_bin(kythe_root: &Path) -> PathBuf {
    kythe_root.join("tools/kythe")
}

/// A spawned warm backend that is killed when this guard drops — so
/// `scry3 repl` can stand up its own `http_server` for the session and tear
/// it down cleanly on exit.
pub struct BackendGuard(std::process::Child);
impl Drop for BackendGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// Start a warm `http_server` on an ephemeral localhost port, wait until it
/// accepts connections, and return `(guard, "localhost:PORT")`. Used by
/// `repl` to make the fast path zero-setup.
pub fn autostart_backend(kythe_root: &Path, serving: &Path) -> Result<(BackendGuard, String)> {
    let bin = kythe_root.join("tools/http_server");
    if !bin.exists() {
        bail!("http_server not found at {}", bin.display());
    }
    // Let the OS pick a free port, then hand it to http_server.
    let port = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").context("pick free port")?;
        l.local_addr()?.port()
    };
    let addr = format!("localhost:{port}");
    let child = Command::new(&bin)
        .arg("--serving_table")
        .arg(serving)
        .arg("--listen")
        .arg(&addr)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .context("spawn http_server")?;
    let guard = BackendGuard(child);
    // Poll until it accepts (LevelDB open is fast; usually <1 s).
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
    while std::time::Instant::now() < deadline {
        if std::net::TcpStream::connect(&addr).is_ok() {
            eprintln!("[repl] warm backend up on {addr}");
            return Ok((guard, addr));
        }
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
    bail!("warm backend did not come up on {addr} within 15s");
}

/// `serve` — run the stock Kythe `http_server` as a warm backend holding the
/// LevelDB serving table open. scry3 query verbs with `--http <listen>` then
/// hit it over a kept-alive socket, paying neither the per-query Go process
/// startup nor a fresh LevelDB open. This is the only way to get scry3 into
/// the sub-millisecond-server-query regime while staying 100% stock Kythe.
pub fn serve(kythe_root: &Path, serving: &Path, listen: &str) -> Result<()> {
    let bin = kythe_root.join("tools/http_server");
    if !bin.exists() {
        bail!("http_server not found at {}", bin.display());
    }
    eprintln!("[serve] warm Kythe backend on {listen} (serving {})", serving.display());
    eprintln!("[serve] query it with:  scry3 --http {listen} def NAME");
    let st = Command::new(&bin)
        .arg("--serving_table")
        .arg(serving)
        .arg("--listen")
        .arg(listen)
        .status()
        .context("spawn http_server")?;
    if !st.success() {
        bail!("http_server exited with {st}");
    }
    Ok(())
}
