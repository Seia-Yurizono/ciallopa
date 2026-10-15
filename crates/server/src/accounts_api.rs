//! Account management API (`/admin/v1/accounts*`): list with status and recent usage (never
//! tokens), enable / disable, weight / priority, remove, import an auth file, device-code
//! login and manual token refresh. Every change needs `[ui] allow_config_write = true` and the
//! same CSRF rules as the reset-credit action; auth files are written under the same locks as
//! token refresh.
use crate::settings::{actor, csrf, err, Settings};
use axum::{
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use ciallopa_auth::file::valid_account_id;
use ciallopa_core::{AccountPatch, AuthError};
use ciallopa_forward::ClientKey;
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

type S = Arc<Settings>;

pub fn routes(s: S) -> Router {
    Router::new()
        .route("/admin/v1/accounts", get(list))
        .route("/admin/v1/accounts/reload", post(reload))
        .route("/admin/v1/accounts/import", post(import))
        .route("/admin/v1/accounts/login/device", post(device_start))
        .route("/admin/v1/accounts/login/{lid}", get(device_status))
        .route(
            "/admin/v1/accounts/{id}",
            axum::routing::patch(update).delete(remove),
        )
        .route("/admin/v1/accounts/{id}/refresh", post(refresh))
        .route(
            "/admin/v1/accounts/{id}/clear-cooldown",
            post(clear_cooldown),
        )
        .with_state(s)
}

fn ms(t: SystemTime) -> i64 {
    t.duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

fn bad_id(id: &str) -> Option<Response> {
    (!valid_account_id(id)).then(|| err(StatusCode::BAD_REQUEST, "invalid account id"))
}

fn auth_err(e: AuthError) -> Response {
    let st = match e {
        AuthError::NotFound(_) => StatusCode::NOT_FOUND,
        AuthError::Invalid(_) => StatusCode::BAD_REQUEST,
        AuthError::ReloginRequired => StatusCode::CONFLICT,
        AuthError::Unimplemented => StatusCode::NOT_IMPLEMENTED,
        AuthError::Refresh(_) => StatusCode::BAD_GATEWAY,
        AuthError::Io(_) => StatusCode::INTERNAL_SERVER_ERROR,
    };
    err(st, &e.to_string())
}

async fn list(State(s): State<S>) -> Response {
    let st = &s.st;
    let relogin = st.accounts.relogin_required().await;
    let mut usage: HashMap<String, Value> = HashMap::new();
    if let Some(rec) = &st.usage {
        let now = ciallopa_usage::now_ms();
        let q = ciallopa_usage::SummaryQuery {
            from_ms: now - 86_400_000,
            to_ms: now,
            group_by: vec![ciallopa_usage::Dim::Account],
            ..Default::default()
        };
        if let Ok(sum) = rec.summary(q).await {
            for g in sum.groups {
                if let Some(Some(a)) = g.group.get("account") {
                    usage.insert(
                        a.clone(),
                        json!({
                            "requests": g.count,
                            "errors": g.errors,
                            "input_tokens": g.input_tokens,
                            "output_tokens": g.output_tokens,
                            "cost_usd": g.cost_usd,
                        }),
                    );
                }
            }
        }
    }
    let mut out = vec![];
    for a in st.accounts.list().await {
        let cool = st.router.cooling_until(&a.id);
        let status = if relogin.contains(&a.id) {
            "token_invalid"
        } else if a.disabled {
            "disabled"
        } else if cool.is_some() {
            "cooling"
        } else {
            "normal"
        };
        out.push(json!({
            "id": a.id,
            "email": a.email,
            "plan": a.plan_type,
            "status": status,
            "disabled": a.disabled,
            "cooling_until_ms": cool.map(ms),
            "cooling_reason": cool.and_then(|_| st.router.cooling_reason(&a.id)),
            "weight": a.weight,
            "priority": a.priority,
            "usage_24h": usage.get(&a.id),
        }));
    }
    let cfg = st.config.load();
    Json(json!({
        "write_enabled": cfg.ui.allow_config_write,
        "strategy": cfg.routing.strategy,
        "usage_enabled": st.usage.is_some(),
        "accounts": out,
    }))
    .into_response()
}

async fn update(
    State(s): State<S>,
    key: Option<axum::Extension<ClientKey>>,
    Path(id): Path<String>,
    headers: HeaderMap,
    body: Option<Json<AccountPatch>>,
) -> Response {
    if let Some(r) = bad_id(&id) {
        return r;
    }
    if let Some(r) = s.write_guard(&headers) {
        return r;
    }
    let Some(Json(p)) = body else {
        return err(
            StatusCode::BAD_REQUEST,
            "body must be JSON {\"disabled\"?, \"weight\"?, \"priority\"?}",
        );
    };
    if p == AccountPatch::default() {
        return err(StatusCode::BAD_REQUEST, "no changes");
    }
    match s.st.accounts.update_account(&id, &p).await {
        Ok(a) => {
            s.audit_account(actor(&key).as_deref(), "account-update", &id, json!(p));
            Json(json!({"ok": true, "account": {"id": a.id, "disabled": a.disabled, "weight": a.weight, "priority": a.priority}}))
                .into_response()
        }
        Err(e) => auth_err(e),
    }
}

#[derive(Deserialize)]
struct ConfirmReq {
    #[serde(default)]
    confirm: String,
}

async fn remove(
    State(s): State<S>,
    key: Option<axum::Extension<ClientKey>>,
    Path(id): Path<String>,
    headers: HeaderMap,
    body: Option<Json<ConfirmReq>>,
) -> Response {
    if let Some(r) = bad_id(&id) {
        return r;
    }
    if let Some(r) = s.write_guard(&headers) {
        return r;
    }
    if body.map(|b| b.0.confirm) != Some(id.clone()) {
        return err(StatusCode::BAD_REQUEST, "confirm must equal the account id");
    }
    match s.st.accounts.remove_account(&id).await {
        Ok(()) => {
            s.cleanup_account(&id);
            s.audit_account(actor(&key).as_deref(), "account-remove", &id, Value::Null);
            Json(json!({"ok": true})).into_response()
        }
        Err(e) => auth_err(e),
    }
}

#[derive(Deserialize)]
struct ImportReq {
    auth_json: Value,
}

async fn import(
    State(s): State<S>,
    key: Option<axum::Extension<ClientKey>>,
    headers: HeaderMap,
    body: Option<Json<ImportReq>>,
) -> Response {
    if let Some(r) = s.write_guard(&headers) {
        return r;
    }
    let Some(Json(req)) = body else {
        return err(
            StatusCode::BAD_REQUEST,
            "body must be JSON {\"auth_json\": {...}}",
        );
    };
    if !req.auth_json.is_object() {
        return err(StatusCode::BAD_REQUEST, "auth_json must be an object");
    }
    match s.st.accounts.import_account(&req.auth_json).await {
        Ok(id) => {
            let _ = s.sync_accounts().await;
            s.audit_account(actor(&key).as_deref(), "account-import", &id, Value::Null);
            Json(json!({"ok": true, "account": id})).into_response()
        }
        Err(e) => auth_err(e),
    }
}

async fn refresh(
    State(s): State<S>,
    key: Option<axum::Extension<ClientKey>>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Response {
    if let Some(r) = bad_id(&id) {
        return r;
    }
    if let Some(r) = s.write_guard(&headers) {
        return r;
    }
    match s.st.accounts.force_refresh(&id).await {
        Ok(_) => {
            s.audit_account(actor(&key).as_deref(), "account-refresh", &id, Value::Null);
            Json(json!({"ok": true})).into_response()
        }
        Err(e) => auth_err(e),
    }
}

/// Manual "clear cooldown": a runtime action (nothing is written to the config), so it needs
/// an admin key and the CSRF header but not `allow_config_write`.
async fn clear_cooldown(
    State(s): State<S>,
    key: Option<axum::Extension<ClientKey>>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Response {
    if let Some(r) = bad_id(&id) {
        return r;
    }
    if let Some(r) = csrf(&headers) {
        return r;
    }
    if !s.st.accounts.list().await.iter().any(|a| a.id == id) {
        return err(StatusCode::NOT_FOUND, "no such account");
    }
    let cleared = s.st.router.clear_cooldown(&id);
    if cleared {
        s.audit_account(
            actor(&key).as_deref(),
            "account-clear-cooldown",
            &id,
            Value::Null,
        );
    }
    Json(json!({"ok": true, "cleared": cleared})).into_response()
}

async fn reload(State(s): State<S>, headers: HeaderMap) -> Response {
    if let Some(r) = csrf(&headers) {
        return r;
    }
    match s.sync_accounts().await {
        Ok(gone) => Json(json!({"ok": true, "removed": gone})).into_response(),
        Err(e) => err(StatusCode::INTERNAL_SERVER_ERROR, &e),
    }
}

// ---------------- device-code login ----------------

static LOGINS: LazyLock<Mutex<HashMap<String, Value>>> = LazyLock::new(Default::default);

fn logins() -> std::sync::MutexGuard<'static, HashMap<String, Value>> {
    LOGINS.lock().unwrap_or_else(|e| e.into_inner())
}

/// Start the existing device-code flow (`ciallopa login`): returns the verification URL and
/// one-time code; the server polls in the background and saves the account when approved.
async fn device_start(
    State(s): State<S>,
    key: Option<axum::Extension<ClientKey>>,
    headers: HeaderMap,
) -> Response {
    if let Some(r) = s.write_guard(&headers) {
        return r;
    }
    if logins()
        .values()
        .any(|v| v.get("status").and_then(Value::as_str) == Some("pending"))
    {
        return err(
            StatusCode::CONFLICT,
            "a device login is already in progress",
        );
    }
    let cfg = s.st.config.load();
    let oauth = match crate::oauth_client(&cfg.proxy_url) {
        Ok(o) => o,
        Err(e) => return err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
    };
    let lid = format!("{:016x}", rand::random::<u64>());
    let (tx, rx) = tokio::sync::oneshot::channel::<(String, String)>();
    let dir = cfg.auth_dir.clone();
    let (s2, lid2, who) = (s.clone(), lid.clone(), actor(&key));
    logins().insert(lid.clone(), json!({"status": "starting"}));
    tokio::spawn(async move {
        let mut tx = Some(tx);
        let r = ciallopa_auth::login::device_code_with(
            &oauth,
            &dir,
            ciallopa_auth::login::DEVICE_CODE_MAX_WAIT,
            |url, code| {
                if let Some(t) = tx.take() {
                    let _ = t.send((url.to_string(), code.to_string()));
                }
            },
        )
        .await;
        let v = match r {
            Ok(id) => {
                let _ = s2.sync_accounts().await;
                s2.audit_account(who.as_deref(), "account-login", &id, Value::Null);
                json!({"status": "done", "account": id})
            }
            Err(e) => json!({"status": "error", "message": e.to_string()}),
        };
        logins().insert(lid2, v);
    });
    match tokio::time::timeout(Duration::from_secs(30), rx).await {
        Ok(Ok((url, code))) => {
            let v = json!({"status": "pending", "url": url, "code": code});
            logins().insert(lid.clone(), v.clone());
            let mut out = v;
            out["login_id"] = json!(lid);
            Json(out).into_response()
        }
        _ => {
            let msg = logins()
                .get(&lid)
                .and_then(|v| v.get("message").and_then(Value::as_str).map(str::to_owned))
                .unwrap_or_else(|| "device code request failed".into());
            logins().remove(&lid);
            err(StatusCode::BAD_GATEWAY, &msg)
        }
    }
}

async fn device_status(Path(lid): Path<String>) -> Response {
    match logins().get(&lid) {
        Some(v) => Json(v.clone()).into_response(),
        None => err(StatusCode::NOT_FOUND, "unknown login"),
    }
}
