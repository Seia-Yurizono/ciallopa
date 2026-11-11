//! Every error code a usage row can carry has exactly one Chinese explanation in the admin
//! UI (shown next to the English code), and the UI has no label for a code nothing records.
#![allow(clippy::unwrap_used)]
use std::collections::BTreeSet;

fn keys(html: &str, start: &str) -> BTreeSet<String> {
    let i = html
        .find(start)
        .unwrap_or_else(|| panic!("{start} not in ui.html"))
        + start.len();
    let body = &html[i..i + html[i..].find('}').unwrap()];
    body.split(',')
        .filter_map(|kv| kv.split_once(':'))
        .map(|(k, _)| k.trim().to_string())
        .collect()
}

#[test]
fn error_labels_cover_codes_one_to_one() {
    let html = include_str!("../src/ui.html");
    let mut labelled = keys(html, "const ERR_LBL=Object.assign({");
    labelled.extend(keys(html, "const PROXY_ERR={"));
    let codes = ciallopa_forward::codes::LOCAL
        .iter()
        .chain(ciallopa_forward::codes::UPSTREAM)
        .chain(ciallopa_forward::codes::LEGACY)
        .map(|s| s.to_string())
        .collect::<BTreeSet<_>>();
    let missing: Vec<_> = codes.difference(&labelled).collect();
    let dead: Vec<_> = labelled.difference(&codes).collect();
    assert!(
        missing.is_empty() && dead.is_empty(),
        "unlabelled: {missing:?}; labels for codes nothing records: {dead:?}"
    );
}

#[test]
fn new_codex_terminal_codes_have_chinese_labels() {
    const CODES: &[&str] = &[
        "inference_hook_denied",
        "content_policy_violation",
        "invalid_access_program",
        "unsupported_access_program",
        "access_program_not_enabled",
        "invalid_workspace_selected",
        "codex_workspace_access_denied",
        "budget_reached",
    ];
    let html = include_str!("../src/ui.html");
    let labelled = keys(html, "const ERR_LBL=Object.assign({");
    for code in CODES {
        assert!(
            ciallopa_forward::codes::UPSTREAM.contains(code),
            "{code} missing from codes::UPSTREAM"
        );
        assert!(labelled.contains(*code), "{code} missing ERR_LBL");
    }
}
