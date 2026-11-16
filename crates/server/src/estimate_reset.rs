//! Manual quota-estimate reset and undo (admin). The usage writer applies both in order with
//! the records queued before them; nothing is deleted.
use crate::http::{csrf, err, require_account};
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::{Json, Router};
use ciallopa_forward::{AppState, ClientKey};
use ciallopa_usage::{UsageError, WindowKind};
use serde::Deserialize;
use serde_json::json;

pub fn routes(st: AppState) -> Router {
    Router::new()
        .route("/admin/v1/accounts/{id}/quota-estimate/reset", post(reset))
        .route(
            "/admin/v1/accounts/{id}/quota-estimate/reset/{epoch}/undo",
            post(undo),
        )
        .with_state(st)
}

#[derive(Deserialize)]
struct ResetReq {
    /// `primary`, `secondary` or `all`.
    window: String,
    /// Must equal the account id in the path (explicit confirmation).
    #[serde(default)]
    confirm: String,
}

fn window(s: &str) -> Result<Option<WindowKind>, ()> {
    match s {
        "primary" => Ok(Some(WindowKind::Primary)),
        "secondary" => Ok(Some(WindowKind::Secondary)),
        "all" => Ok(None),
        _ => Err(()),
    }
}

async fn reset(
    State(st): State<AppState>,
    key: Option<axum::Extension<ClientKey>>,
    Path(account): Path<String>,
    headers: HeaderMap,
    body: Option<Json<ResetReq>>,
) -> Response {
    if let Some(r) = csrf(&headers) {
        return r;
    }
    let Some(Json(req)) = body else {
        return err(
            StatusCode::BAD_REQUEST,
            "body must be JSON {\"window\": \"primary|secondary|all\", \"confirm\": \"<account>\"}",
        );
    };
    if req.confirm != account {
        return err(StatusCode::BAD_REQUEST, "confirm must equal the account id");
    }
    let Ok(which) = window(&req.window) else {
        return err(
            StatusCode::BAD_REQUEST,
            "window must be primary, secondary or all",
        );
    };
    let Some(rec) = st.usage.clone() else {
        return err(StatusCode::NOT_FOUND, "usage log disabled");
    };
    if let Err(r) = require_account(&st, &account).await {
        return r;
    }
    let label = key.as_ref().map(|k| k.0.label.to_string());
    match rec.reset_estimate(&account, which, label).await {
        Ok(done) => Json(done).into_response(),
        Err(UsageError::NotFound(_)) => err(
            StatusCode::NOT_FOUND,
            "no quota snapshot recorded for this account yet",
        ),
        Err(e) => err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
    }
}

async fn undo(
    State(st): State<AppState>,
    Path((account, epoch)): Path<(String, i64)>,
    headers: HeaderMap,
) -> Response {
    if let Some(r) = csrf(&headers) {
        return r;
    }
    let Some(rec) = st.usage.clone() else {
        return err(StatusCode::NOT_FOUND, "usage log disabled");
    };
    if let Err(r) = require_account(&st, &account).await {
        return r;
    }
    match rec.undo_estimate_reset(&account, epoch).await {
        Ok(n) => Json(json!({"epoch_id": epoch, "undone": n})).into_response(),
        Err(UsageError::NotFound(_)) => err(
            StatusCode::NOT_FOUND,
            "reset not found, already undone, or older than 30 days",
        ),
        Err(e) => err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
    }
}
