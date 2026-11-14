use crate::errors::json_error;
use axum::response::Response;
use base64::Engine;
use http::{HeaderMap, HeaderValue, StatusCode};
use serde_json::{json, Map, Value};
use std::collections::BTreeMap;

pub(super) const DEFAULT_TOOL_MODEL: &str = "gpt-image-2";

pub(super) const DIRECT_MODELS: &[&str] = &[
    "gpt-image-1.5",
    "gpt-image-2",
    "gpt-image-2.5",
    "gpt-image-2.5-flare",
    "gpt-image-2.5-sunburst",
];

#[derive(Clone, Copy, PartialEq)]
pub(super) enum Kind {
    Generate,
    Edit,
}

#[derive(Default)]
pub(super) struct Form {
    values: BTreeMap<String, Vec<String>>,
    files: BTreeMap<String, Vec<String>>, // data URLs
}

pub(super) fn bad(msg: &str) -> Response {
    json_error(StatusCode::BAD_REQUEST, "invalid_request_error", msg)
}

fn sniff_mime(b: &[u8]) -> &'static str {
    if b.starts_with(b"\x89PNG") {
        "image/png"
    } else if b.starts_with(&[0xff, 0xd8, 0xff]) {
        "image/jpeg"
    } else if b.len() > 12 && &b[..4] == b"RIFF" && &b[8..12] == b"WEBP" {
        "image/webp"
    } else if b.starts_with(b"GIF8") {
        "image/gif"
    } else {
        "application/octet-stream"
    }
}

/// Minimal RFC 7578 multipart/form-data parser (fields + files, in memory).
pub(super) fn parse_multipart(ct: &str, body: &[u8]) -> Result<Form, String> {
    let boundary = ct
        .split(';')
        .filter_map(|p| p.trim().split_once('='))
        .find(|(k, _)| k.trim().eq_ignore_ascii_case("boundary"))
        .map(|(_, v)| v.trim().trim_matches('"').to_string())
        .filter(|b| !b.is_empty())
        .ok_or("multipart boundary is required")?;
    let delim = format!("--{boundary}");
    let d = delim.as_bytes();
    let find = |hay: &[u8], from: usize, n: &[u8]| -> Option<usize> {
        hay.get(from..)?
            .windows(n.len())
            .position(|w| w == n)
            .map(|p| p + from)
    };
    let mut f = Form::default();
    let mut pos = find(body, 0, d).ok_or("missing first boundary")? + d.len();
    loop {
        if body.get(pos..pos + 2) == Some(b"--") {
            break;
        }
        let part_start = pos + 2; // CRLF after boundary
        let next = find(body, part_start, d).ok_or("unterminated multipart body")?;
        let mut part = body.get(part_start..next).ok_or("bad part")?;
        if part.ends_with(b"\r\n") {
            part = &part[..part.len() - 2];
        }
        let hdr_end = find(part, 0, b"\r\n\r\n").ok_or("bad part headers")?;
        let head = String::from_utf8_lossy(&part[..hdr_end]);
        let data = &part[hdr_end + 4..];
        let (mut name, mut filename, mut ctype) = (String::new(), None, None);
        for line in head.split("\r\n") {
            let Some((k, v)) = line.split_once(':') else {
                continue;
            };
            if k.trim().eq_ignore_ascii_case("content-disposition") {
                for p in v.split(';').map(str::trim) {
                    if let Some(x) = p.strip_prefix("name=") {
                        name = x.trim_matches('"').trim().to_string();
                    } else if let Some(x) = p.strip_prefix("filename=") {
                        filename = Some(x.trim_matches('"').to_string());
                    }
                }
            } else if k.trim().eq_ignore_ascii_case("content-type") {
                ctype = Some(v.trim().to_string());
            }
        }
        if filename.is_some() {
            let mime = ctype.unwrap_or_else(|| sniff_mime(data).to_string());
            let url = format!(
                "data:{mime};base64,{}",
                base64::engine::general_purpose::STANDARD.encode(data)
            );
            f.files.entry(name).or_default().push(url);
        } else {
            f.values
                .entry(name)
                .or_default()
                .push(String::from_utf8_lossy(data).into_owned());
        }
        pos = next + d.len();
    }
    Ok(f)
}

impl Form {
    pub(super) fn get(&self, k: &str) -> Option<&str> {
        self.values
            .get(k)
            .and_then(|v| v.first())
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
    }
    pub(super) fn images(&self) -> Vec<String> {
        self.files
            .get("image[]")
            .or_else(|| self.files.get("image"))
            .cloned()
            .unwrap_or_default()
    }
    pub(super) fn mask(&self) -> Option<String> {
        self.files.get("mask").and_then(|v| v.first()).cloned()
    }
}

fn form_json_value(key: &str, v: &str) -> Value {
    let v = v.trim();
    if matches!(key, "n" | "output_compression" | "partial_images") {
        if let Ok(n) = v.parse::<i64>() {
            return json!(n);
        }
    }
    json!(v)
}

/// CPA `codexRewriteOpenAIImageEditMultipartToJSON`.
pub(super) fn multipart_edit_to_json(f: &Form, model: &str, stream: bool) -> Value {
    let mut out = Map::new();
    out.insert("model".into(), json!(model));
    if stream {
        out.insert("stream".into(), json!(true));
    }
    for (k, vals) in &f.values {
        if k.is_empty() || k == "model" || k == "stream" || vals.is_empty() {
            continue;
        }
        let path = match k.as_str() {
            "mask[file_id]" => "mask.file_id",
            "mask[image_url]" => "mask.image_url",
            other => other,
        };
        let val = if vals.len() == 1 {
            form_json_value(path, &vals[0])
        } else {
            Value::Array(vals.iter().map(|v| form_json_value(k, v)).collect())
        };
        if let Some((a, b)) = path.split_once('.') {
            let e = out.entry(a.to_string()).or_insert_with(|| json!({}));
            if let Some(o) = e.as_object_mut() {
                o.insert(b.into(), val);
            }
        } else {
            out.insert(path.into(), val);
        }
    }
    if let Some(m) = f.mask() {
        let e = out.entry("mask").or_insert_with(|| json!({}));
        if let Some(o) = e.as_object_mut() {
            o.insert("image_url".into(), json!(m));
        }
    }
    let files = f.images();
    if !files.is_empty() {
        let mut items = match out.remove("images") {
            Some(Value::Array(a)) => a,
            _ => vec![],
        };
        items.extend(files.into_iter().map(|u| json!({"image_url": u})));
        out.insert("images".into(), Value::Array(items));
    }
    Value::Object(out)
}

/// CPA `codexBuildOpenAIImageTool` (JSON) / multipart variant.
pub(super) fn build_tool(
    kind: Kind,
    model: &str,
    get_s: &dyn Fn(&str) -> Option<String>,
    get_n: &dyn Fn(&str) -> Option<i64>,
) -> Map<String, Value> {
    let mut t = Map::new();
    t.insert("type".into(), json!("image_generation"));
    t.insert(
        "action".into(),
        json!(if kind == Kind::Edit {
            "edit"
        } else {
            "generate"
        }),
    );
    t.insert("model".into(), json!(model));
    let mut fields = vec!["size", "quality", "background", "output_format"];
    if kind == Kind::Edit {
        fields.push("input_fidelity");
    }
    fields.push("moderation");
    for k in fields {
        if let Some(v) = get_s(k) {
            t.insert(k.into(), json!(v));
        }
    }
    for k in ["output_compression", "partial_images"] {
        if let Some(v) = get_n(k) {
            t.insert(k.into(), json!(v));
        }
    }
    t
}

/// CPA `codexBuildImagesResponsesRequest`.
pub(super) fn responses_request(
    main_model: &str,
    prompt: &str,
    images: &[String],
    tool: Map<String, Value>,
) -> Value {
    let mut content = vec![json!({"type": "input_text", "text": prompt})];
    content.extend(
        images
            .iter()
            .filter(|s| !s.trim().is_empty())
            .map(|u| json!({"type": "input_image", "image_url": u})),
    );
    json!({
        "instructions": "",
        "stream": true,
        "reasoning": {"effort": "medium", "summary": "auto"},
        "parallel_tool_calls": true,
        "include": ["reasoning.encrypted_content"],
        "model": main_model,
        "store": false,
        "tool_choice": {"type": "image_generation"},
        "tools": [Value::Object(tool)],
        "input": [{"type": "message", "role": "user", "content": content}],
    })
}

pub(super) fn out_headers(
    cfg: &ciallopa_core::config::ImagesConfig,
    client: &HeaderMap,
    accept_sse: bool,
) -> HeaderMap {
    let mut h = HeaderMap::new();
    for (k, v) in client.iter() {
        if matches!(
            k.as_str(),
            "content-type" | "content-encoding" | "content-length" | "accept"
        ) {
            continue;
        }
        h.append(k.clone(), v.clone());
    }
    if !h.contains_key("originator") {
        if let Ok(v) = HeaderValue::from_str(&cfg.default_originator) {
            h.insert("originator", v);
        }
    }
    if !client
        .get(http::header::USER_AGENT)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|u| u.starts_with("codex"))
    {
        if let Ok(v) = HeaderValue::from_str(&cfg.default_user_agent) {
            h.insert(http::header::USER_AGENT, v);
        }
    }
    h.insert(
        http::header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    h.insert(
        http::header::ACCEPT,
        HeaderValue::from_static(if accept_sse {
            "text/event-stream"
        } else {
            "application/json"
        }),
    );
    h
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn edit_form_to_json() {
        let mut f = Form::default();
        f.values.insert("prompt".into(), vec!["p".into()]);
        f.values.insert("n".into(), vec!["2".into()]);
        f.values.insert("model".into(), vec!["x".into()]);
        f.files
            .insert("image[]".into(), vec!["data:image/png;base64,AA==".into()]);
        f.files
            .insert("mask".into(), vec!["data:image/png;base64,BB==".into()]);
        let j = multipart_edit_to_json(&f, "gpt-image-2", true);
        assert_eq!(j["model"], "gpt-image-2");
        assert_eq!(j["n"], 2);
        assert_eq!(j["stream"], true);
        assert_eq!(j["images"][0]["image_url"], "data:image/png;base64,AA==");
        assert_eq!(j["mask"]["image_url"], "data:image/png;base64,BB==");
    }
}
