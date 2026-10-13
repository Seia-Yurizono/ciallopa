//! Session identity extraction — port of CPA `sdk/cliproxy/session/info.go`
//! (`ExtractSessionInfo`) + `selector.go` (`extractSessionIDs`, message-hash fallback).
use cpa_lite_core::SessionInfo;
use http::HeaderMap;
use serde_json::Value;
use sha2::{Digest, Sha256};

/// CPA `NormalizeExplicitID`: reject control chars, trim, 1..=256 bytes.
pub fn normalize_explicit_id(raw: &str) -> Option<String> {
    if raw.chars().any(char::is_control) {
        return None;
    }
    let t = raw.trim();
    if t.is_empty() || t.len() > 256 {
        return None;
    }
    Some(t.to_string())
}

/// CPA `BoundSessionIdentity`: ids > 256 bytes become `<190-byte utf8 prefix>#<sha256 hex>`.
pub fn bound_session_identity(id: &str) -> String {
    if id.len() <= 256 {
        return id.to_string();
    }
    let hash = hex(&Sha256::digest(id.as_bytes()));
    let mut n = (255 - 1 - hash.len()).min(id.len());
    while n > 0 && !id.is_char_boundary(n) {
        n -= 1;
    }
    format!("{}#{}", &id[..n], hash)
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

/// gjson `.String()` semantics.
fn gstr(v: Option<&Value>) -> String {
    match v {
        None | Some(Value::Null) => String::new(),
        Some(Value::String(s)) => s.clone(),
        Some(Value::Bool(b)) => b.to_string(),
        Some(Value::Number(n)) => n.to_string(),
        Some(o) => o.to_string(),
    }
}

/// Dotted-path lookup (gjson style, objects only).
fn get<'a>(v: Option<&'a Value>, path: &str) -> Option<&'a Value> {
    let mut cur = v?;
    for seg in path.split('.') {
        cur = cur.as_object()?.get(seg)?;
    }
    Some(cur)
}

fn cand(v: Option<&Value>, path: &str) -> Option<String> {
    normalize_explicit_id(&gstr(get(v, path)))
}

struct Body<'a> {
    root: Option<&'a Value>,
    req: Option<&'a Value>, // nested `request` (when no `contents`)
}
impl<'a> Body<'a> {
    fn new(b: Option<&'a Value>) -> Self {
        let root = b.filter(|v| !v.is_null());
        let req = match root {
            Some(r) if get(Some(r), "request").is_some() && get(Some(r), "contents").is_none() => {
                get(Some(r), "request")
            }
            _ => None,
        };
        Body { root, req }
    }
    /// root path, then nested request path.
    fn first(&self, paths: &[&str]) -> Option<String> {
        for p in paths {
            if let Some(v) = cand(self.root, p) {
                return Some(v);
            }
            if self.req.is_some() {
                if let Some(v) = cand(self.req, p) {
                    return Some(v);
                }
            }
        }
        None
    }
    fn exists(&self) -> bool {
        self.root.is_some()
    }
}

/// CPA `sessionHeaderValue`: first value (any casing) that normalizes.
fn hv(h: &HeaderMap, name: &str) -> Option<String> {
    h.get_all(name)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .find_map(normalize_explicit_id)
}
fn hv_any(h: &HeaderMap, names: &[&str]) -> Option<String> {
    names.iter().find_map(|n| hv(h, n))
}

#[derive(Default, Debug, Clone, PartialEq)]
pub(crate) struct RawInfo {
    pub session_id: String,
    pub parent: String,
    pub is_fork: bool,
}

const PARENT_PATHS: &[&str] = &[
    "parent_session_id", "parentSessionId", "parentSessionID",
    "parent_thread_id", "parentThreadId", "parentThreadID",
    "forked_from_thread_id", "forked_from_id",
    "parent_conversation_id", "parentConversationId", "parentConversationID",
    "parent_id", "parentId", "parentID",
    "parent_task_id", "parentTaskId", "parentTaskID",
    "parent_action_id", "parentActionId", "parentActionID",
    "parent_session", "parentSession",
    "parent_subagent_id", "parentSubagentId",
    "forkSource.sessionId", "fork_source.session_id",
    "previousSessionId", "previous_session_id",
    "metadata.parent_session_id", "metadata.parentSessionId", "metadata.parentSessionID",
    "metadata.parent_thread_id", "metadata.parentThreadId",
    "metadata.forked_from_thread_id", "metadata.forked_from_id",
    "metadata.parent_id", "metadata.parentId", "metadata.parentID",
    "metadata.parent_task_id", "metadata.parentTaskId", "metadata.parentTaskID",
    "metadata.parent_action_id", "metadata.parentActionId",
    "metadata.parent_subagent_id", "metadata.parentSubagentId",
    "metadata.parent_session", "metadata.parentSession",
    "metadata.parent_agent_id", "metadata.parentAgentId",
    "metadata.forkSource.sessionId", "metadata.previousSessionId",
    "extra_body.parent_session_id", "extra_body.parentSessionId", "extra_body.parentSessionID",
    "extra_body.parent_thread_id", "extra_body.parentThreadId",
    "extra_body.forked_from_thread_id", "extra_body.forked_from_id",
    "extra_body.parent_id", "extra_body.parentId", "extra_body.parentID",
    "extra_body.parent_task_id", "extra_body.parentTaskId",
    "extra_body.parent_action_id", "extra_body.parentActionId",
    "extra_body.parent_subagent_id", "extra_body.parentSubagentId",
    "extra_body.parent_session", "extra_body.parentSession",
];

const FORK_BODY_PATHS: &[&str] = &[
    "forked_from_thread_id", "forked_from_id",
    "forkSource.sessionId", "fork_source.session_id",
    "previousSessionId", "previous_session_id",
    "metadata.forked_from_thread_id", "metadata.forked_from_id",
    "metadata.forkSource.sessionId", "metadata.previousSessionId",
    "extra_body.forked_from_thread_id", "extra_body.forked_from_id",
    "extra_body.forkSource.sessionId", "extra_body.previousSessionId",
];

fn finalize(mut i: RawInfo) -> Option<RawInfo> {
    if i.session_id.is_empty() {
        return None;
    }
    i.session_id = bound_session_identity(&i.session_id);
    if !i.parent.is_empty() {
        i.parent = bound_session_identity(&i.parent);
    }
    if i.parent == i.session_id {
        i.parent.clear();
    }
    Some(i)
}

/// Simple "<prefix>:<sid>" + header parent / body parent candidate family.
fn simple(prefix: &str, sid: &str, parent_hdr: Option<String>, pc: &Option<String>) -> Option<RawInfo> {
    let mut i = RawInfo { session_id: format!("{prefix}:{sid}"), ..Default::default() };
    if let Some(p) = parent_hdr.filter(|p| p != sid) {
        i.parent = format!("{prefix}:{p}");
    } else if let Some(p) = pc.as_ref().filter(|p| *p != sid) {
        i.parent = format!("{prefix}:{p}");
    }
    finalize(i)
}

/// CPA `ExtractSessionInfo` minus Claude-identity / execution / LCP branches.
pub(crate) fn extract_info(h: &HeaderMap, body: Option<&Value>) -> Option<RawInfo> {
    let b = Body::new(body);
    let pc = if b.exists() { b.first(PARENT_PATHS) } else { None };

    // ---- Codex ----
    let mut sid = hv_any(h, &["session-id", "session_id"]);
    let mut tid = hv_any(h, &["thread-id", "thread_id"]);
    let meta: Option<Value> = h
        .get("x-codex-turn-metadata")
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .and_then(|s| serde_json::from_str(s).ok());
    let m = meta.as_ref();
    if sid.is_none() && m.is_some() {
        sid = cand(m, "session_id");
    }
    if tid.is_none() && m.is_some() {
        tid = cand(m, "thread_id");
    }
    if tid.is_none() && sid.is_some() && b.exists() {
        tid = b.first(&["thread_id", "threadId", "metadata.thread_id"]);
    }
    if sid.is_some() || tid.is_some() {
        let parent_thread = hv(h, "x-codex-parent-thread-id").or_else(|| cand(m, "parent_thread_id"));
        let mut forked = cand(m, "forked_from_thread_id").or_else(|| cand(m, "forked_from_id"));
        if forked.is_none() && b.exists() {
            forked = b.first(&[
                "forked_from_thread_id", "forked_from_id",
                "metadata.forked_from_thread_id", "metadata.forked_from_id",
                "extra_body.forked_from_thread_id", "extra_body.forked_from_id",
            ]);
        }
        let agent = m
            .map(|m| {
                let raw = gstr(get(Some(m), "agent_name"));
                let raw = raw.strip_prefix("/root/").unwrap_or(&raw);
                let raw = raw.strip_prefix('/').unwrap_or(raw);
                normalize_explicit_id(raw.trim()).unwrap_or_default()
            })
            .filter(|n| !n.is_empty() && n != "root" && n != "main");
        let sub = hv(h, "x-openai-subagent");
        let mut sub_sig = sub.is_some_and(|v| !v.eq_ignore_ascii_case("false") && v != "0");
        if gstr(get(m, "subagent_kind")) == "thread_spawn" {
            sub_sig = true;
        }
        let s = sid.clone().unwrap_or_default();
        let t = tid.clone().unwrap_or_default();
        if let Some(f) = forked {
            let mut fsid = if t.is_empty() { s.clone() } else { t.clone() };
            if fsid == f && !s.is_empty() && s != f {
                fsid = s.clone();
            }
            return finalize(RawInfo {
                session_id: format!("codex:{fsid}"),
                parent: format!("codex:{f}"),
                is_fork: true,
            });
        }
        let pt = parent_thread.clone().unwrap_or_default();
        if sub_sig || (!t.is_empty() && !s.is_empty() && t != s) || (!pt.is_empty() && pt != t && pt != s) {
            let child = if t.is_empty() { s.clone() } else { t.clone() };
            let psid = if pt.is_empty() { s.clone() } else { pt.clone() };
            let mut i = RawInfo::default();
            if let (Some(a), false) = (&agent, s.is_empty()) {
                i.session_id = format!("codex:{s}:agent:{a}");
                if !psid.is_empty() {
                    i.parent = format!("codex:{psid}");
                } else if let Some(p) = pc.as_ref().filter(|p| **p != s) {
                    i.parent = format!("codex:{p}");
                }
            } else {
                i.session_id = format!("codex:{child}");
                if !psid.is_empty() && psid != child {
                    i.parent = format!("codex:{psid}");
                } else if let Some(p) = pc.as_ref().filter(|p| **p != child) {
                    i.parent = format!("codex:{p}");
                }
            }
            return finalize(i);
        }
        let session = if s.is_empty() { t } else { s };
        let mut i = RawInfo { session_id: format!("codex:{session}"), ..Default::default() };
        if !pt.is_empty() && pt != session {
            i.parent = format!("codex:{pt}");
        } else if let Some(p) = pc.as_ref().filter(|p| **p != session) {
            i.parent = format!("codex:{p}");
        }
        return finalize(i);
    }

    // ---- other explicit harness headers (same order as CPA) ----
    const P: &[&str] = &["x-parent-session-id", "x-parent-id"];
    if let Some(s) = hv(h, "x-http-session-id") {
        return simple("agy", &s, hv_any(h, P), &pc);
    }
    if let Some(s) = hv(h, "x-session-id") {
        return simple("header", &s, hv_any(h, P), &pc);
    }
    if let Some(s) = hv(h, "x-session-affinity") {
        return simple("affinity", &s, hv_any(h, &["x-parent-session-affinity", "x-parent-session-id", "x-parent-id"]), &pc);
    }
    if let Some(s) = hv(h, "x-slot-session-id") {
        return simple("slot", &s, hv_any(h, &["x-parent-slot-session-id", "x-parent-session-id", "x-parent-id"]), &pc);
    }
    if let Some(s) = hv_any(h, &["x-task-id", "x-task_id"]) {
        return simple("task", &s, hv_any(h, &["x-parent-task-id", "x-parent-session-id", "x-parent-id"]), &pc);
    }
    if let Some(s) = hv(h, "x-conversation-id") {
        return simple("conv", &s, hv_any(h, &["x-parent-conversation-id", "x-parent-id"]), &pc);
    }
    if let Some(s) = hv(h, "x-thread-id") {
        return simple("thread", &s, hv_any(h, &["x-parent-thread-id", "x-parent-id"]), &pc);
    }
    if let Some(s) = hv(h, "x-client-request-id") {
        return simple("clientreq", &s, hv_any(h, P), &pc);
    }

    // ---- body ids ----
    if !b.exists() {
        return None;
    }
    let is_fork_body = || b.first(FORK_BODY_PATHS).is_some();
    let body_child = |prefix: &str, id: &str| {
        let mut i = RawInfo { session_id: format!("{prefix}:{id}"), ..Default::default() };
        if let Some(p) = pc.as_ref().filter(|p| *p != id) {
            i.parent = format!("{prefix}:{p}");
            i.is_fork = is_fork_body();
        }
        finalize(i)
    };
    if let Some(c) = b.first(&["cachedContent"]).or_else(|| b.first(&["cached_content"])) {
        return simple("geminicache", &c, None, &pc);
    }
    for p in ["thread_id", "threadId", "metadata.thread_id"] {
        if let Some(t) = b.first(&[p]) {
            return body_child("thread", &t);
        }
    }
    let agent = cand(b.root, "metadata.agent_id")
        .or_else(|| cand(b.root, "metadata.subagent_id"))
        .or_else(|| hv_any(h, &["x-claude-code-agent-id", "x-agent-id"]))
        .or_else(|| cand(b.req, "metadata.agent_id"))
        .or_else(|| cand(b.req, "metadata.subagent_id"));
    for p in [
        "session_id", "sessionId", "sessionID", "child_session_id", "childSessionId",
        "metadata.session_id", "metadata.sessionId", "metadata.sessionID", "metadata.child_session_id",
        "extra_body.session_id", "extra_body.sessionId", "extra_body.sessionID",
    ] {
        if let Some(s) = b.first(&[p]) {
            if let Some(a) = agent.as_ref().filter(|a| *a != "main") {
                let mut i = RawInfo {
                    session_id: format!("session:{s}:agent:{a}"),
                    parent: format!("session:{s}"),
                    is_fork: false,
                };
                if let Some(p) = pc.as_ref().filter(|p| **p != s) {
                    i.parent = format!("session:{p}");
                }
                return finalize(i);
            }
            return body_child("session", &s);
        }
    }
    for p in [
        "task_id", "taskId", "taskID", "action_id", "actionId", "actionID",
        "metadata.task_id", "metadata.taskId", "metadata.taskID",
        "metadata.action_id", "metadata.actionId", "metadata.actionID",
        "extra_body.task_id", "extra_body.taskId", "extra_body.taskID",
    ] {
        if let Some(t) = b.first(&[p]) {
            return body_child("task", &t);
        }
    }
    let conversation_id = conversation_alias(&b);
    if let Some(pck) = b.first(&["prompt_cache_key"]).or_else(|| b.first(&["promptCacheKey"])) {
        return simple("pck", &pck, None, &pc);
    }
    if let Some(cid) = conversation_id {
        let mut i = RawInfo { session_id: cid.clone(), ..Default::default() };
        if let Some(p) = pc.as_ref().filter(|p| format!("conv:{p}") != cid) {
            i.parent = format!("conv:{p}");
        }
        return finalize(i);
    }
    if let Some(u) = b.first(&["metadata.user_id"]) {
        return finalize(RawInfo { session_id: format!("user:{u}"), ..Default::default() });
    }
    for p in ["conversation_id", "conversationId", "chat_id", "chatId", "metadata.conversation_id", "extra_body.conversation_id"] {
        if let Some(c) = b.first(&[p]) {
            return simple("conv", &c, None, &pc);
        }
    }
    None
}

/// `conversation.id` / string `conversation` → `conv:<id>`.
fn conversation_alias(b: &Body) -> Option<String> {
    let mut conv = get(b.root, "conversation");
    if conv.is_none() {
        conv = get(b.req, "conversation");
    }
    let conv = conv?;
    if let Some(id) = cand(Some(conv), "id") {
        return Some(format!("conv:{id}"));
    }
    if let Value::String(s) = conv {
        return normalize_explicit_id(s).map(|id| format!("conv:{id}"));
    }
    None
}

/// CPA `isHierarchyParent`.
pub(crate) fn is_hierarchy_parent(primary: &str, fallback: &str) -> bool {
    if fallback.is_empty() || primary.is_empty() || primary == fallback {
        return false;
    }
    if primary.contains(":agent:") {
        return true;
    }
    match (primary.find(':'), fallback.find(':')) {
        (Some(a), Some(b)) if a > 0 && b > 0 => primary[..a] == fallback[..b],
        (None, None) => true,
        _ => false,
    }
}

/// CPA `isSubagentSession`.
pub(crate) fn is_subagent_session(primary: &str, fallback: &str) -> bool {
    if primary.contains(":agent:") {
        return true;
    }
    if fallback.is_empty() || primary.is_empty() || primary == fallback {
        return false;
    }
    is_hierarchy_parent(primary, fallback)
}

/// Full CPA order → `SessionInfo { key: primary, parent_key: fallback, is_subagent, is_fork }`.
/// `parent_key` is the CPA "fallback id": explicit parent/fork source, or the conversation
/// alias for `pck:` sessions, or the short message hash.
pub fn extract_session(headers: &HeaderMap, body: Option<&Value>) -> SessionInfo {
    let (primary, fallback, is_fork) = if let Some(i) = extract_info(headers, body) {
        let mut fb = i.parent.clone();
        if fb.is_empty() && i.session_id.starts_with("pck:") {
            fb = conversation_alias(&Body::new(body)).unwrap_or_default();
        }
        (i.session_id, fb, i.is_fork)
    } else if let Some(b) = body {
        let (p, f) = message_hash_ids(b);
        (p, f, false)
    } else {
        (String::new(), String::new(), false)
    };
    if primary.is_empty() {
        return SessionInfo::default();
    }
    let primary = bound_session_identity(&primary);
    let fallback = if fallback.is_empty() { String::new() } else { bound_session_identity(&fallback) };
    let is_subagent = !is_fork && is_subagent_session(&primary, &fallback);
    SessionInfo {
        key: Some(primary),
        parent_key: (!fallback.is_empty()).then_some(fallback),
        is_subagent,
        is_fork,
    }
}


fn trunc(s: &str) -> &[u8] {
    let b = s.as_bytes();
    &b[..b.len().min(100)]
}

fn fnv64a(parts: &[&[u8]]) -> u64 {
    let mut h: u64 = 0xcbf29ce484222325;
    for p in parts {
        for &c in *p {
            h ^= c as u64;
            h = h.wrapping_mul(0x100000001b3);
        }
    }
    h
}

fn session_hash(sys: &[u8], usr: &[u8], ast: &[u8]) -> String {
    let mut parts: Vec<Vec<u8>> = vec![];
    for (tag, v) in [(&b"sys:"[..], sys), (b"usr:", usr), (b"ast:", ast)] {
        if !v.is_empty() {
            let mut x = tag.to_vec();
            x.extend_from_slice(v);
            x.push(b'\n');
            parts.push(x);
        }
    }
    let refs: Vec<&[u8]> = parts.iter().map(|p| p.as_slice()).collect();
    format!("msg:{:016x}", fnv64a(&refs))
}

fn texts_of(content: Option<&Value>, types: &[&str]) -> String {
    match content {
        Some(Value::String(s)) => s.clone(),
        Some(Value::Array(a)) => a
            .iter()
            .filter(|p| types.contains(&gstr(get(Some(p), "type")).as_str()))
            .map(|p| gstr(get(Some(p), "text")))
            .filter(|t| !t.is_empty())
            .collect::<Vec<_>>()
            .join(" "),
        _ => String::new(),
    }
}

/// CPA `extractMessageHashIDs` (byte-exact FNV-64a over first 100 bytes).
pub(crate) fn message_hash_ids(payload: &Value) -> (String, String) {
    let (mut sys, mut usr, mut ast) = (Vec::<u8>::new(), Vec::<u8>::new(), Vec::<u8>::new());
    let set = |slot: &mut Vec<u8>, s: &str| {
        if slot.is_empty() {
            *slot = trunc(s).to_vec();
        }
    };
    let root = Some(payload);
    if let Some(Value::Array(msgs)) = get(root, "messages") {
        for m in msgs {
            let role = gstr(get(Some(m), "role"));
            let c = texts_of(get(Some(m), "content"), &["text"]);
            if c.is_empty() {
                continue;
            }
            match role.as_str() {
                "system" => set(&mut sys, &c),
                "user" => set(&mut usr, &c),
                "assistant" => set(&mut ast, &c),
                _ => {}
            }
            if !sys.is_empty() && !usr.is_empty() && !ast.is_empty() {
                break;
            }
        }
    }
    if sys.is_empty() {
        match get(root, "system") {
            Some(Value::Array(a)) => {
                if let Some(t) = a.iter().map(|p| gstr(get(Some(p), "text"))).find(|t| !t.is_empty()) {
                    set(&mut sys, &t);
                }
            }
            Some(Value::String(s)) => set(&mut sys, s),
            _ => {}
        }
    }
    if sys.is_empty() && usr.is_empty() {
        if let Some(Value::Array(a)) = get(root, "systemInstruction.parts") {
            if let Some(t) = a.iter().map(|p| gstr(get(Some(p), "text"))).find(|t| !t.is_empty()) {
                set(&mut sys, &t);
            }
        }
        if let Some(Value::Array(cs)) = get(root, "contents") {
            for m in cs {
                let role = gstr(get(Some(m), "role"));
                if let Some(Value::Array(parts)) = get(Some(m), "parts") {
                    if let Some(t) = parts.iter().map(|p| gstr(get(Some(p), "text"))).find(|t| !t.is_empty()) {
                        match role.as_str() {
                            "user" => set(&mut usr, &t),
                            "model" => set(&mut ast, &t),
                            _ => {}
                        }
                    }
                }
                if !usr.is_empty() && !ast.is_empty() {
                    break;
                }
            }
        }
    }
    if sys.is_empty() && usr.is_empty() {
        let instr = gstr(get(root, "instructions"));
        if !instr.is_empty() {
            set(&mut sys, &instr);
        }
        if let Some(Value::Array(items)) = get(root, "input") {
            for it in items {
                let ty = gstr(get(Some(it), "type"));
                if ty == "reasoning" || (!ty.is_empty() && ty != "message") {
                    continue;
                }
                let role = gstr(get(Some(it), "role"));
                if ty.is_empty() && role.is_empty() {
                    continue;
                }
                let t = texts_of(get(Some(it), "content"), &["input_text", "output_text", "text"]);
                if t.is_empty() {
                    continue;
                }
                match role.as_str() {
                    "developer" | "system" => set(&mut sys, &t),
                    "user" => set(&mut usr, &t),
                    "assistant" => set(&mut ast, &t),
                    _ => {}
                }
                if !usr.is_empty() && !ast.is_empty() {
                    break;
                }
            }
        }
    }
    if usr.is_empty() {
        return (String::new(), String::new());
    }
    let short = session_hash(&sys, &usr, &[]);
    if ast.is_empty() {
        return (short, String::new());
    }
    (session_hash(&sys, &usr, &ast), short)
}
