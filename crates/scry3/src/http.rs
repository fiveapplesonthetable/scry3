//! A tiny HTTP/1.1 client (no dependencies) for talking to a warm Kythe
//! `http_server` over a keep-alive connection.
//!
//! Why this exists: the `kythe` CLI is fast *internally* (a warm serving
//! query is ~500 µs) but every invocation pays ~40 ms of Go runtime
//! startup. The fix is to keep one Go process warm (`scry3 serve`, which
//! holds the LevelDB serving table open) and have scry3 hit it directly —
//! no per-query process spawn. Reusing the TCP connection across queries
//! removes the connect+handshake cost too, so the per-query overhead
//! collapses to localhost round-trip + the ~500 µs server query.
//!
//! The server's JSON reply is byte-identical to `kythe --json`, so the
//! same reshaping code in `query.rs` consumes both transports unchanged.

use anyhow::{bail, Context, Result};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;

pub struct HttpClient {
    addr: String,
    conn: Option<BufReader<TcpStream>>,
}

impl HttpClient {
    pub fn new(addr: impl Into<String>) -> Self {
        // Accept "host:port", "http://host:port", or a trailing slash.
        let mut a = addr.into();
        if let Some(rest) = a.strip_prefix("http://") {
            a = rest.to_string();
        }
        let a = a.trim_end_matches('/').to_string();
        HttpClient { addr: a, conn: None }
    }

    fn stream(&mut self) -> Result<&mut BufReader<TcpStream>> {
        if self.conn.is_none() {
            let s = TcpStream::connect(&self.addr)
                .with_context(|| format!("connect to warm server {}", self.addr))?;
            s.set_nodelay(true).ok();
            self.conn = Some(BufReader::new(s));
        }
        Ok(self.conn.as_mut().unwrap())
    }

    /// POST `body` (JSON) to `path`, return the response body bytes. Retries
    /// once on a dropped keep-alive connection.
    pub fn post(&mut self, path: &str, body: &str) -> Result<Vec<u8>> {
        match self.post_once(path, body) {
            Ok(v) => Ok(v),
            Err(_) => {
                self.conn = None; // stale keep-alive; reconnect and retry once
                self.post_once(path, body)
            }
        }
    }

    fn post_once(&mut self, path: &str, body: &str) -> Result<Vec<u8>> {
        let addr = self.addr.clone();
        let host = addr.split(':').next().unwrap_or("localhost");
        let req = format!(
            "POST /{} HTTP/1.1\r\nHost: {}\r\nContent-Type: application/json\r\n\
             Content-Length: {}\r\nConnection: keep-alive\r\n\r\n{}",
            path.trim_start_matches('/'),
            host,
            body.len(),
            body
        );
        let r = self.stream()?;
        r.get_mut().write_all(req.as_bytes()).context("write request")?;
        r.get_mut().flush().ok();

        // Status line.
        let mut status = String::new();
        if r.read_line(&mut status)? == 0 {
            bail!("connection closed before status");
        }
        let code: u16 = status
            .split_whitespace()
            .nth(1)
            .and_then(|c| c.parse().ok())
            .unwrap_or(0);

        // Headers.
        let mut content_len: Option<usize> = None;
        let mut chunked = false;
        loop {
            let mut line = String::new();
            if r.read_line(&mut line)? == 0 {
                break;
            }
            let line = line.trim_end();
            if line.is_empty() {
                break;
            }
            let lower = line.to_ascii_lowercase();
            if let Some(v) = lower.strip_prefix("content-length:") {
                content_len = v.trim().parse().ok();
            } else if lower.starts_with("transfer-encoding:") && lower.contains("chunked") {
                chunked = true;
            }
        }

        // Body.
        let body_bytes = if chunked {
            read_chunked(r)?
        } else if let Some(n) = content_len {
            let mut buf = vec![0u8; n];
            r.read_exact(&mut buf).context("read body")?;
            buf
        } else {
            // No length and not chunked: read to EOF (and the connection is
            // now unusable for keep-alive).
            let mut buf = Vec::new();
            r.read_to_end(&mut buf).ok();
            self.conn = None;
            buf
        };

        if code >= 400 {
            bail!("server returned HTTP {code}: {}", String::from_utf8_lossy(&body_bytes));
        }
        Ok(body_bytes)
    }
}

fn read_chunked(r: &mut BufReader<TcpStream>) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    loop {
        let mut sz = String::new();
        if r.read_line(&mut sz)? == 0 {
            break;
        }
        let n = usize::from_str_radix(sz.trim().split(';').next().unwrap_or("0").trim(), 16)
            .context("parse chunk size")?;
        if n == 0 {
            // trailing CRLF
            let mut _t = String::new();
            let _ = r.read_line(&mut _t);
            break;
        }
        let mut buf = vec![0u8; n];
        r.read_exact(&mut buf).context("read chunk")?;
        out.extend_from_slice(&buf);
        let mut _crlf = [0u8; 2];
        r.read_exact(&mut _crlf).ok();
    }
    Ok(out)
}
