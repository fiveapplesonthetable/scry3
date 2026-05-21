//! Name extraction from Kythe nodes.
//!
//! Two sources of a human-typeable name for a node:
//!
//!   * `/kythe/edge/named` — Java/JVM/Go indexers emit this; the target
//!     VName's `signature` IS the qualified name (with a trailing JVM
//!     method descriptor for methods, which we strip).
//!   * `/kythe/code` — the cxx_indexer emits no `named` edge, so the
//!     human name is a `MarkedSource` proto under this fact. We render it
//!     to a flat FQN like `android::Parcel::writeStrongBinder`.
//!
//! The `MarkedSource` parser and the JVM-descriptor stripper are lifted
//! verbatim from scry2's `kythe.rs` — they are pure functions over byte
//! buffers, the trusted core of scry2's name resolution. scry3 reuses them
//! to build the one thing the open-source Kythe serving table lacks: a
//! name → ticket index (Kythe's `IdentifierMatch` table is Google-internal
//! and never written by the OSS `write_tables`).

pub fn is_named_edge(kind: &str) -> bool {
    let base = kind.split('.').next().unwrap_or(kind);
    base == "/kythe/edge/named"
}

const MS_BOX: u32 = 0;
const MS_TYPE: u32 = 1;
const MS_IDENTIFIER: u32 = 3;
const MS_CONTEXT: u32 = 4;

/// Parse a Kythe `MarkedSource` proto and render it to a flat C++ FQN like
/// `android::Parcel::writeStrongBinder`. Parameter lists are truncated at
/// the first `(`. Returns None on a malformed proto or empty rendering.
pub fn parse_marked_source_fqn(buf: &[u8]) -> Option<String> {
    fn render(buf: &[u8]) -> Option<(String, u32)> {
        let mut kind: u32 = MS_BOX;
        let mut pre = String::new();
        let mut joiner = String::new();
        let mut post = String::new();
        let mut add_final_list_token = false;
        let mut child_renders: Vec<String> = Vec::new();
        let mut pos = 0;
        while pos < buf.len() {
            let (field, wire, val_end, val_start) = read_proto_field(buf, pos)?;
            pos = val_end;
            match (field, wire) {
                (1, 0) => {
                    if let Some((v, _)) = read_varint_at(&buf[val_start..val_end], 0) {
                        kind = v as u32;
                    }
                }
                (2, 2) => pre = String::from_utf8_lossy(&buf[val_start..val_end]).into_owned(),
                (3, 2) => {
                    if let Some((s, _)) = render(&buf[val_start..val_end]) {
                        child_renders.push(s);
                    }
                }
                (4, 2) => joiner = String::from_utf8_lossy(&buf[val_start..val_end]).into_owned(),
                (5, 2) => post = String::from_utf8_lossy(&buf[val_start..val_end]).into_owned(),
                (10, 0) => {
                    if let Some((v, _)) = read_varint_at(&buf[val_start..val_end], 0) {
                        add_final_list_token = v != 0;
                    }
                }
                _ => {}
            }
        }
        let mut out = String::with_capacity(pre.len() + post.len());
        out.push_str(&pre);
        if !child_renders.is_empty() {
            out.push_str(&child_renders.join(&joiner));
            if add_final_list_token {
                out.push_str(&joiner);
            }
        }
        out.push_str(&post);
        if out.is_empty() {
            None
        } else {
            Some((out, kind))
        }
    }
    let (full, _) = render(buf)?;
    let cut = full.find('(').unwrap_or(full.len());
    let trimmed = full[..cut].trim_end();
    let last_token = trimmed
        .rsplit_once(char::is_whitespace)
        .map(|(_, fqn)| fqn)
        .unwrap_or(trimmed);
    let fqn = last_token.trim_end_matches(':');
    if fqn.is_empty() {
        None
    } else {
        Some(fqn.to_string())
    }
}

#[allow(dead_code)]
const _MS_KINDS_DOC: (u32, u32, u32) = (MS_TYPE, MS_IDENTIFIER, MS_CONTEXT);

fn read_proto_field(buf: &[u8], mut pos: usize) -> Option<(u32, u8, usize, usize)> {
    let (tag, p1) = read_varint_at(buf, pos)?;
    pos = p1;
    let field = (tag >> 3) as u32;
    let wire = (tag & 0x7) as u8;
    let (val_start, val_end) = match wire {
        0 => {
            let (_, p2) = read_varint_at(buf, pos)?;
            (pos, p2)
        }
        1 => {
            let end = pos.checked_add(8).filter(|&e| e <= buf.len())?;
            (pos, end)
        }
        2 => {
            let (len, p2) = read_varint_at(buf, pos)?;
            let end = p2.checked_add(len as usize)?;
            if end > buf.len() {
                return None;
            }
            (p2, end)
        }
        5 => {
            let end = pos.checked_add(4).filter(|&e| e <= buf.len())?;
            (pos, end)
        }
        _ => return None,
    };
    Some((field, wire, val_end, val_start))
}

fn read_varint_at(buf: &[u8], mut pos: usize) -> Option<(u64, usize)> {
    let mut val: u64 = 0;
    let mut shift = 0u32;
    for _ in 0..10 {
        if pos >= buf.len() {
            return None;
        }
        let b = buf[pos];
        pos += 1;
        val |= ((b & 0x7F) as u64) << shift;
        if b & 0x80 == 0 {
            return Some((val, pos));
        }
        shift += 7;
    }
    None
}

/// Strip the trailing JVM method descriptor from a Java named-edge
/// signature (`...clearCallingIdentity()J` → `...clearCallingIdentity`).
/// Returns `Some(prefix)` when a descriptor is found, else `None`.
pub fn strip_jvm_method_descriptor(sig: &str) -> Option<&str> {
    let open = sig.rfind('(')?;
    let bytes = sig.as_bytes();
    let close_rel = bytes[open..].iter().position(|&b| b == b')')?;
    let close = open + close_rel;
    if !bytes[open + 1..close].iter().all(is_jvm_type_byte) {
        return None;
    }
    let ret = &bytes[close + 1..];
    if ret.is_empty() || !ret.iter().all(is_jvm_type_byte) {
        return None;
    }
    Some(&sig[..open])
}

fn is_jvm_type_byte(b: &u8) -> bool {
    matches!(*b,
        b'B' | b'C' | b'D' | b'F' | b'I' | b'J' | b'S' | b'V' | b'Z'
        | b'L' | b';' | b'[' | b'/' | b'$' | b'_'
        | b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9')
}

/// Decode standard base64 (the encoding `entrystream --write_format=json`
/// uses for `fact_value`). Ignores embedded whitespace; tolerant of
/// missing padding. Returns None on an invalid character.
pub fn base64_decode(s: &str) -> Option<Vec<u8>> {
    fn val(c: u8) -> Option<u32> {
        match c {
            b'A'..=b'Z' => Some((c - b'A') as u32),
            b'a'..=b'z' => Some((c - b'a' + 26) as u32),
            b'0'..=b'9' => Some((c - b'0' + 52) as u32),
            b'+' => Some(62),
            b'/' => Some(63),
            _ => None,
        }
    }
    let mut out = Vec::with_capacity(s.len() * 3 / 4);
    let mut acc: u32 = 0;
    let mut nbits = 0u32;
    for &c in s.as_bytes() {
        if c == b'=' || c == b'\n' || c == b'\r' || c == b' ' || c == b'\t' {
            continue;
        }
        let v = val(c)?;
        acc = (acc << 6) | v;
        nbits += 6;
        if nbits >= 8 {
            nbits -= 8;
            out.push((acc >> nbits) as u8);
        }
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base64_roundtrip_known() {
        // "meta" → "bWV0YQ==" (seen in real entrystream JSON output)
        assert_eq!(base64_decode("bWV0YQ==").unwrap(), b"meta");
    }

    #[test]
    fn jvm_descriptor_stripped() {
        assert_eq!(
            strip_jvm_method_descriptor("android.os.Binder.clearCallingIdentity()J"),
            Some("android.os.Binder.clearCallingIdentity")
        );
        assert_eq!(strip_jvm_method_descriptor("android.os.Binder"), None);
    }
}
