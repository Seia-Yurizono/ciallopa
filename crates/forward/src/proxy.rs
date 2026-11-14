use http::HeaderMap;

/// Bound zstd inspection; forward oversized decoded bodies uninspected.
pub(crate) fn inspect_json(
    headers: &HeaderMap,
    body: &[u8],
    limit: u64,
) -> Option<serde_json::Value> {
    let enc = headers
        .get(http::header::CONTENT_ENCODING)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .trim()
        .to_ascii_lowercase();
    match enc.as_str() {
        "" | "identity" => serde_json::from_slice(body).ok(),
        "zstd" => {
            let mut out = Vec::new();
            let mut dec = zstd::stream::read::Decoder::new(body).ok()?;
            use std::io::Read;
            (&mut dec)
                .take(limit.saturating_add(1))
                .read_to_end(&mut out)
                .ok()?;
            if out.len() as u64 > limit {
                return None;
            }
            serde_json::from_slice(&out).ok()
        }
        _ => None,
    }
}
