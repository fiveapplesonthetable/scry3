//! Kythe `VName` → ticket URI, byte-for-byte compatible with Kythe's
//! `go/util/kytheuri`. The `kythe` CLI addresses every node by this ticket
//! string, so the name index must emit exactly the form Kythe parses back.
//!
//! Canonical form (empty components omitted):
//!   `kythe://<corpus>?lang=<lang>?path=<path>?root=<root>#<signature>`
//! `corpus`/`root`/`path` use the `paths` escaper (does NOT escape `/`);
//! `lang`/`signature` use the `all` escaper (escapes `/`). `path` is also
//! lightly cleaned, mirroring `cleanPath`.

use serde::Deserialize;

/// Subset of `kythe.proto.VName` carried in `entrystream --write_format=json`.
#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq, Hash)]
#[serde(default)]
pub struct VName {
    pub signature: String,
    pub corpus: String,
    pub root: String,
    pub path: String,
    pub language: String,
}

const HEX: &[u8; 16] = b"0123456789ABCDEF";

/// `escape_all`: percent-escape everything except unreserved (incl. `/`).
fn escape(s: &str, keep_slash: bool) -> String {
    fn unreserved(c: u8) -> bool {
        c.is_ascii_alphanumeric() || matches!(c, b'-' | b'.' | b'_' | b'~')
    }
    let needs = s.bytes().any(|c| !unreserved(c) && !(keep_slash && c == b'/'));
    if !needs {
        return s.to_string();
    }
    let mut out = Vec::with_capacity(s.len() + 8);
    for c in s.bytes() {
        if unreserved(c) || (keep_slash && c == b'/') {
            out.push(c);
        } else {
            out.push(b'%');
            out.push(HEX[(c >> 4) as usize]);
            out.push(HEX[(c & 0xf) as usize]);
        }
    }
    String::from_utf8(out).unwrap()
}

/// Minimal equivalent of Go `path.Clean` for the relative paths Kythe
/// indexers emit. Collapses `//`, resolves `.` / `..` segments.
fn clean_path(p: &str) -> String {
    if p.is_empty() {
        return String::new();
    }
    let rooted = p.starts_with('/');
    let mut out: Vec<&str> = Vec::new();
    for seg in p.split('/') {
        match seg {
            "" | "." => {}
            ".." => {
                if let Some(last) = out.last() {
                    if *last != ".." {
                        out.pop();
                        continue;
                    }
                }
                if !rooted {
                    out.push("..");
                }
            }
            s => out.push(s),
        }
    }
    let joined = out.join("/");
    if rooted {
        format!("/{joined}")
    } else if joined.is_empty() {
        ".".to_string()
    } else {
        joined
    }
}

impl VName {
    /// Render the canonical Kythe ticket URI.
    pub fn to_ticket(&self) -> String {
        let mut s = String::from("kythe:");
        if !self.corpus.is_empty() {
            s.push_str("//");
            s.push_str(&escape(&self.corpus, true));
        }
        if !self.language.is_empty() {
            s.push_str("?lang=");
            s.push_str(&escape(&self.language, false));
        }
        if !self.path.is_empty() {
            s.push_str("?path=");
            s.push_str(&escape(&clean_path(&self.path), true));
        }
        if !self.root.is_empty() {
            s.push_str("?root=");
            s.push_str(&escape(&self.root, true));
        }
        if !self.signature.is_empty() {
            s.push('#');
            s.push_str(&escape(&self.signature, false));
        }
        s
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cxx_ticket_matches_kythe_decor_output() {
        // From real `kythe decor` output on our serving table:
        //   kythe://android.googlesource.com/platform/superproject?lang=c%2B%2B?path=frameworks/native/libs/binder/Parcel.cpp#%23init
        let v = VName {
            signature: "#init".into(),
            corpus: "android.googlesource.com/platform/superproject".into(),
            root: "".into(),
            path: "frameworks/native/libs/binder/Parcel.cpp".into(),
            language: "c++".into(),
        };
        assert_eq!(
            v.to_ticket(),
            "kythe://android.googlesource.com/platform/superproject?lang=c%2B%2B?path=frameworks/native/libs/binder/Parcel.cpp#%23init"
        );
    }

    #[test]
    fn empty_components_omitted() {
        let v = VName {
            signature: "sig".into(),
            corpus: "c".into(),
            ..Default::default()
        };
        assert_eq!(v.to_ticket(), "kythe://c#sig");
    }
}
