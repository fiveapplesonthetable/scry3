//! `scry3` — a stock-Kythe wrapper for AOSP-scale code walks.
//!
//! Where scry2 replaced Kythe's whole serving half with a custom `.s2db`,
//! scry3 keeps it stock: the serving table is built by `write_tables` and
//! every cross-reference/edge/node query goes through the `kythe` CLI. scry3
//! adds exactly two things on top:
//!
//!   1. orchestration of the indexing stage (kzip → per-CU `Entry` streams),
//!      forked from scry2's battle-tested `from-kzip` dispatch, and
//!   2. a name → ticket index — the one capability the open-source Kythe
//!      serving table lacks (its `IdentifierMatch` table is never written by
//!      the OSS `write_tables`).
//!
//! Pipeline:  `index` → `build` → `name-index` → query verbs.

mod build;
mod http;
mod indexer;
mod kzip;
mod marked_source;
mod nameindex;
mod query;
mod stream;
mod ticket;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use std::cell::RefCell;
use std::path::{Path, PathBuf};

#[derive(Parser, Debug)]
#[command(name = "scry3", version, about = "stock-Kythe wrapper for AOSP")]
struct Cli {
    /// Path to the Kythe release root (contains tools/ and indexers/).
    /// Defaults to $KYTHE_ROOT.
    #[arg(long, global = true)]
    kythe_root: Option<PathBuf>,

    /// Path to the LevelDB serving table (for query verbs). `--index` is
    /// accepted as a scry2-style alias.
    #[arg(long, global = true, visible_alias = "index")]
    serving: Option<PathBuf>,

    /// Path to the scry3 name index. Defaults to <serving>/scry3.names.idx.
    #[arg(long, global = true)]
    name_index: Option<PathBuf>,

    /// Query a warm `scry3 serve` backend at ADDR (host:port) instead of
    /// spawning the kythe CLI per call — the fast path.
    #[arg(long, global = true)]
    http: Option<String>,

    /// Emit the raw kythe JSON reply instead of terse lines.
    #[arg(long, global = true)]
    json: bool,

    /// Cap on results / substring matches.
    #[arg(long, global = true, default_value = "50")]
    limit: usize,

    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand, Debug)]
enum Cmd {
    /// kzip → directory of per-CU `<sha>.entries` (runs the patched Kythe
    /// indexers, one subprocess per compilation unit).
    Index {
        #[arg(long)]
        kzip: PathBuf,
        #[arg(long = "out-entries")]
        out_entries: PathBuf,
        #[arg(long, default_value = "cxx,java,jvm,go,proto,textproto")]
        langs: String,
        #[arg(long, default_value = "8g")]
        jvm_heap: String,
        #[arg(long = "in", value_name = "SUBSTR", num_args = 1.., value_delimiter = ',')]
        in_: Vec<String>,
        #[arg(long = "not-in", value_name = "SUBSTR", num_args = 1.., value_delimiter = ',')]
        not_in: Vec<String>,
        #[arg(long)]
        staging: Option<PathBuf>,
        #[arg(long, default_value = "0")]
        workers: usize,
        /// `PREFIX::ARG` — prepend ARG to the compiler argv of any CU whose
        /// primary path starts with PREFIX (e.g. libcore --patch-module).
        #[arg(long = "inject-cu-arg", value_name = "PREFIX::ARG")]
        inject_cu_args: Vec<String>,
        /// Skip CUs whose `<sha>.entries` already exists.
        #[arg(long)]
        resume: bool,
    },

    /// entries directory → Kythe LevelDB serving table (via write_tables).
    Build {
        #[arg(long = "entries")]
        entries: PathBuf,
        #[arg(short, long)]
        out: PathBuf,
        /// sorted (default, fast for a slice) | graphstore (memory-safe for
        /// all of AOSP) | beam (only with a real Beam runner).
        #[arg(long, default_value = "sorted")]
        mode: String,
        #[arg(long, default_value = "0")]
        workers: usize,
        #[arg(long)]
        work: Option<PathBuf>,
        #[arg(long)]
        keep_intermediate: bool,
    },

    /// index-stream — kzip → serving table in ONE pass with bounded disk:
    /// each CU's entries are streamed straight into a GraphStore (deduped on
    /// disk) and deleted, never materializing the full entry set. The
    /// AOSP-scale path. Also builds the name index in the same pass.
    IndexStream {
        #[arg(long)]
        kzip: PathBuf,
        /// Output serving table directory.
        #[arg(short, long)]
        out: PathBuf,
        /// GraphStore scratch dir (LevelDB). Default <out>.graphstore.
        #[arg(long)]
        graphstore: Option<PathBuf>,
        /// Name index output. Default <out>/scry3.names.idx. Empty to skip.
        #[arg(long)]
        names: Option<PathBuf>,
        #[arg(long, default_value = "cxx,java,jvm,go,proto,textproto")]
        langs: String,
        #[arg(long, default_value = "8g")]
        jvm_heap: String,
        #[arg(long = "in", value_name = "SUBSTR", num_args = 1.., value_delimiter = ',')]
        in_: Vec<String>,
        #[arg(long = "not-in", value_name = "SUBSTR", num_args = 1.., value_delimiter = ',')]
        not_in: Vec<String>,
        #[arg(long)]
        staging: Option<PathBuf>,
        #[arg(long, default_value = "0")]
        workers: usize,
        #[arg(long = "inject-cu-arg", value_name = "PREFIX::ARG")]
        inject_cu_args: Vec<String>,
        /// Keep the GraphStore after building (default: delete it).
        #[arg(long)]
        keep_graphstore: bool,
    },

    /// entries directory → name → ticket index (scry3's sidecar).
    NameIndex {
        #[arg(long = "entries")]
        entries: PathBuf,
        #[arg(short, long)]
        out: PathBuf,
        #[arg(long, default_value = "0")]
        workers: usize,
    },

    /// def NAME — definition site(s) of a symbol (or ticket).
    Def {
        name: String,
        #[arg(long)]
        substr: bool,
        #[arg(long = "in", value_name = "SUBSTR")]
        in_: Option<String>,
        #[arg(long = "not-in", value_name = "SUBSTR")]
        not_in: Option<String>,
    },
    /// ref NAME — every reference to a symbol.
    Ref {
        name: String,
        #[arg(long)]
        substr: bool,
        #[arg(long = "in", value_name = "SUBSTR")]
        in_: Option<String>,
        #[arg(long = "not-in", value_name = "SUBSTR")]
        not_in: Option<String>,
    },
    /// callers NAME — call sites targeting a function.
    Callers {
        name: String,
        #[arg(long)]
        substr: bool,
        #[arg(long = "in", value_name = "SUBSTR")]
        in_: Option<String>,
        #[arg(long = "not-in", value_name = "SUBSTR")]
        not_in: Option<String>,
    },
    /// super NAME — direct supertypes (extends / overrides / satisfies).
    Super {
        name: String,
        #[arg(long)]
        substr: bool,
    },
    /// sub NAME — direct subtypes.
    Sub {
        name: String,
        #[arg(long)]
        substr: bool,
    },
    /// identifier NAME — list tickets a name resolves to (scry3 index).
    Identifier {
        name: String,
        #[arg(long)]
        substr: bool,
    },
    /// names PREFIX — substring-list the name index (diagnostic).
    Names { prefix: String },
    /// stat — serving table + name index size/health.
    Stat,
    /// repl — stdin/stdout loop with the name index (and warm --http
    /// connection, if set) kept open across queries.
    Repl,
    /// serve — run a warm Kythe backend (http_server) holding the serving
    /// table open, so `--http ADDR` queries skip per-call process startup.
    Serve {
        #[arg(long, default_value = "localhost:8089")]
        listen: String,
    },
    /// edges TARGET — outward graph edges (kythe edges).
    Edges {
        target: String,
        #[arg(long)]
        kinds: Option<String>,
    },
    /// nodes TARGET — a node's facts (kythe nodes).
    Nodes { target: String },
    /// decor FILE — a file's decorations (kythe decor).
    Decor {
        file: String,
        #[arg(long)]
        corpus: Option<String>,
    },
    /// ls [PATH] — list corpus / dirs / files.
    Ls { path: Option<String> },
}

fn resolve_kythe_root(cli: &Cli) -> Result<PathBuf> {
    if let Some(r) = &cli.kythe_root {
        return Ok(r.clone());
    }
    if let Some(r) = std::env::var_os("KYTHE_ROOT") {
        return Ok(PathBuf::from(r));
    }
    anyhow::bail!("--kythe-root not set and $KYTHE_ROOT is empty");
}

fn default_name_index(serving: &Path) -> PathBuf {
    serving.join("scry3.names.idx")
}

fn build_ctx(cli: &Cli) -> Result<query::Ctx> {
    let kythe_root = resolve_kythe_root(cli)?;
    let serving = cli
        .serving
        .clone()
        .context("query verbs need --serving <serving table path>")?;
    let kythe_bin = query::default_kythe_bin(&kythe_root);
    if !kythe_bin.exists() {
        anyhow::bail!("kythe binary not found at {}", kythe_bin.display());
    }
    let name_index_path = cli
        .name_index
        .clone()
        .or_else(|| Some(default_name_index(&serving)))
        .filter(|p| p.exists());
    let names = match name_index_path {
        Some(p) => Some(nameindex::NameIndex::load(&p)?),
        None => None,
    };
    let http = cli.http.as_ref().map(|a| RefCell::new(http::HttpClient::new(a.clone())));
    Ok(query::Ctx {
        kythe_bin,
        serving,
        names,
        http,
        json: cli.json,
        limit: cli.limit,
    })
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    match &cli.cmd {
        Cmd::Index {
            kzip,
            out_entries,
            langs,
            jvm_heap,
            in_,
            not_in,
            staging,
            workers,
            inject_cu_args,
            resume,
        } => {
            let kythe_root = resolve_kythe_root(&cli)?;
            let rules = indexer::parse_inject_rules(inject_cu_args)?;
            indexer::run(indexer::IndexArgs {
                kzip,
                kythe_root: &kythe_root,
                entries_dir: out_entries,
                langs,
                jvm_heap,
                in_,
                not_in,
                staging: staging.as_deref(),
                workers: *workers,
                inject_rules: &rules,
                resume: *resume,
            })
        }
        Cmd::Build {
            entries,
            out,
            mode,
            workers,
            work,
            keep_intermediate,
        } => {
            let kythe_root = resolve_kythe_root(&cli)?;
            build::run(build::BuildArgs {
                entries_dir: entries,
                out,
                kythe_root: &kythe_root,
                mode: build::Mode::parse(mode)?,
                workers: *workers,
                work: work.as_deref(),
                keep_intermediate: *keep_intermediate,
            })
        }
        Cmd::IndexStream {
            kzip,
            out,
            graphstore,
            names,
            langs,
            jvm_heap,
            in_,
            not_in,
            staging,
            workers,
            inject_cu_args,
            keep_graphstore,
        } => {
            let kythe_root = resolve_kythe_root(&cli)?;
            let rules = indexer::parse_inject_rules(inject_cu_args)?;
            let gs = graphstore
                .clone()
                .unwrap_or_else(|| out.with_extension("graphstore"));
            // Default name index to <out>/scry3.names.idx; empty string skips.
            let names_path: Option<PathBuf> = match names {
                Some(p) if p.as_os_str().is_empty() => None,
                Some(p) => Some(p.clone()),
                None => Some(out.join("scry3.names.idx")),
            };
            stream::run(stream::StreamArgs {
                kzip,
                kythe_root: &kythe_root,
                out,
                names: names_path.as_deref(),
                graphstore: &gs,
                langs,
                jvm_heap,
                in_,
                not_in,
                staging: staging.as_deref(),
                workers: *workers,
                inject_rules: &rules,
                keep_graphstore: *keep_graphstore,
            })
        }
        Cmd::NameIndex { entries, out, workers } => {
            let kythe_root = resolve_kythe_root(&cli)?;
            nameindex::build(nameindex::NameIndexArgs {
                entries_dir: entries,
                out,
                kythe_root: &kythe_root,
                workers: *workers,
            })
        }
        Cmd::Def { name, substr, in_, not_in } => {
            let f = query::Filter { in_: in_.clone(), not_in: not_in.clone() };
            query::def(&build_ctx(&cli)?, name, *substr, &f)
        }
        Cmd::Ref { name, substr, in_, not_in } => {
            let f = query::Filter { in_: in_.clone(), not_in: not_in.clone() };
            query::references(&build_ctx(&cli)?, name, *substr, &f)
        }
        Cmd::Callers { name, substr, in_, not_in } => {
            let f = query::Filter { in_: in_.clone(), not_in: not_in.clone() };
            query::callers(&build_ctx(&cli)?, name, *substr, &f)
        }
        Cmd::Super { name, substr } => query::inheritance(&build_ctx(&cli)?, name, *substr, false),
        Cmd::Sub { name, substr } => query::inheritance(&build_ctx(&cli)?, name, *substr, true),
        Cmd::Identifier { name, substr } => query::identifier(&build_ctx(&cli)?, name, *substr),
        Cmd::Names { prefix } => query::identifier(&build_ctx(&cli)?, prefix, true),
        Cmd::Stat => query::stat(&build_ctx(&cli)?),
        Cmd::Repl => {
            let mut ctx = build_ctx(&cli)?;
            // Self-contained fast path: if the user didn't point us at a
            // backend, stand one up for the session and tear it down on exit.
            let _backend = if ctx.http.is_none() {
                let kythe_root = resolve_kythe_root(&cli)?;
                let (guard, addr) = query::autostart_backend(&kythe_root, &ctx.serving)?;
                ctx.http = Some(RefCell::new(http::HttpClient::new(addr)));
                Some(guard)
            } else {
                None
            };
            query::repl(&ctx)
        }
        Cmd::Serve { listen } => {
            let kythe_root = resolve_kythe_root(&cli)?;
            let serving = cli
                .serving
                .clone()
                .context("serve needs --serving <serving table path>")?;
            query::serve(&kythe_root, &serving, listen)
        }
        Cmd::Edges { target, kinds } => query::edges(&build_ctx(&cli)?, target, kinds.as_deref()),
        Cmd::Nodes { target } => query::nodes(&build_ctx(&cli)?, target),
        Cmd::Decor { file, corpus } => query::decor(&build_ctx(&cli)?, file, corpus.as_deref()),
        Cmd::Ls { path } => query::ls(&build_ctx(&cli)?, path.as_deref()),
    }
}
