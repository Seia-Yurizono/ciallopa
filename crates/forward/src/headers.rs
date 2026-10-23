//! Header policy: transparent passthrough, replace only `authorization` + `chatgpt-account-id`.
use ciallopa_core::Credentials;
use http::header::{HeaderMap, HeaderName, HeaderValue};

/// Request headers never forwarded upstream (hop-by-hop, local client key, proxy metadata,
/// WS handshake fields that the upstream dialer regenerates).
fn drop_request(name: &str) -> bool {
    matches!(
        name,
        "host"
            | "connection"
            | "keep-alive"
            | "proxy-connection"
            | "proxy-authorization"
            | "proxy-authenticate"
            | "te"
            | "trailer"
            | "transfer-encoding"
            | "upgrade"
            | "content-length"
            | "x-api-key"
            | "forwarded"
            | "via"
            | "x-real-ip"
            | "cdn-loop"
            // client cookies never go upstream (optional per-account jar is added separately)
            | "cookie"
    ) || name.starts_with("x-forwarded-")
        || name.starts_with("sec-websocket-")
        || name.starts_with("cf-")
}

fn connection_listed(h: &HeaderMap) -> Vec<String> {
    h.get_all(http::header::CONNECTION)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(','))
        .map(|s| s.trim().to_ascii_lowercase())
        .filter(|s| !s.is_empty())
        .collect()
}

/// Build upstream headers in client order. `authorization` / `chatgpt-account-id` are replaced
/// in place; if the client sent no `authorization`, both are appended at the end.
pub fn upstream_headers(client: &HeaderMap, creds: &Credentials) -> HeaderMap {
    let listed = connection_listed(client);
    let mut out = HeaderMap::with_capacity(client.len() + 2);
    let bearer = HeaderValue::from_str(&format!("Bearer {}", creds.access_token)).ok();
    let acct = creds
        .chatgpt_account_id
        .as_deref()
        .and_then(|a| HeaderValue::from_str(a).ok());
    let acct_name = HeaderName::from_static("chatgpt-account-id");
    let mut wrote_auth = false;
    let mut wrote_acct = false;
    for (name, value) in client.iter() {
        let n = name.as_str();
        if drop_request(n) || listed.iter().any(|l| l == n) {
            continue;
        }
        if name == http::header::AUTHORIZATION {
            if wrote_auth {
                continue;
            }
            wrote_auth = true;
            if let Some(b) = &bearer {
                out.append(http::header::AUTHORIZATION, b.clone());
            }
            if !client.contains_key(&acct_name) && !wrote_acct {
                if let Some(a) = &acct {
                    out.append(acct_name.clone(), a.clone());
                }
                wrote_acct = true;
            }
            continue;
        }
        if name == acct_name {
            if !wrote_acct {
                if let Some(a) = &acct {
                    out.append(acct_name.clone(), a.clone());
                }
                wrote_acct = true;
            }
            continue;
        }
        out.append(name.clone(), value.clone());
    }
    if !wrote_auth {
        if let Some(b) = bearer {
            out.append(http::header::AUTHORIZATION, b);
        }
    }
    if !wrote_acct {
        if let Some(a) = acct {
            out.append(acct_name, a);
        }
    }
    out
}

/// Response headers copied downstream minus hop-by-hop.
pub fn downstream_headers(up: &HeaderMap) -> HeaderMap {
    let mut out = HeaderMap::with_capacity(up.len());
    for (name, value) in up.iter() {
        let n = name.as_str();
        if matches!(
            n,
            "connection"
                | "keep-alive"
                | "proxy-connection"
                | "te"
                | "trailer"
                | "transfer-encoding"
                | "upgrade"
                // upstream cookies (__cf_bm, __oailb) belong to the shared account session
                | "set-cookie"
                | "set-cookie2"
        ) || n.starts_with("sec-websocket-")
        {
            continue;
        }
        out.append(name.clone(), value.clone());
    }
    out
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    fn creds() -> Credentials {
        Credentials {
            account: "a".into(),
            access_token: "tok".into(),
            chatgpt_account_id: Some("acc-1".into()),
        }
    }

    #[test]
    fn order_preserved_and_replaced() {
        let mut h = HeaderMap::new();
        for (k, v) in [
            ("originator", "codex_cli_rs"),
            ("authorization", "Bearer sk-cpa-local"),
            ("chatgpt-account-id", "client-acc"),
            ("x-forwarded-for", "1.2.3.4"),
            ("connection", "keep-alive, x-drop"),
            ("x-drop", "1"),
            ("session_id", "s1"),
            ("content-encoding", "zstd"),
            ("x-api-key", "k"),
        ] {
            h.append(HeaderName::from_static(k), HeaderValue::from_static(v));
        }
        let o = upstream_headers(&h, &creds());
        let got: Vec<_> = o
            .iter()
            .map(|(k, v)| format!("{}={}", k, v.to_str().unwrap()))
            .collect();
        assert_eq!(
            got,
            vec![
                "originator=codex_cli_rs",
                "authorization=Bearer tok",
                "chatgpt-account-id=acc-1",
                "session_id=s1",
                "content-encoding=zstd",
            ]
        );
    }

    #[test]
    fn set_cookie_stripped() {
        let mut h = HeaderMap::new();
        h.append("set-cookie", HeaderValue::from_static("__cf_bm=x"));
        h.append(
            "x-codex-primary-used-percent",
            HeaderValue::from_static("1"),
        );
        let o = downstream_headers(&h);
        assert!(o.get("set-cookie").is_none());
        assert!(o.get("x-codex-primary-used-percent").is_some());
    }

    #[test]
    fn appended_when_missing() {
        let mut h = HeaderMap::new();
        h.insert("x-api-key", HeaderValue::from_static("k"));
        h.insert("accept", HeaderValue::from_static("*/*"));
        let o = upstream_headers(&h, &creds());
        let names: Vec<_> = o.keys().map(|k| k.as_str()).collect();
        assert_eq!(names, vec!["accept", "authorization", "chatgpt-account-id"]);
    }
}
