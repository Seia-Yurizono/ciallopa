#![allow(clippy::unwrap_used)]
use axum::body::{to_bytes, Body};
use ciallopa_core::Config;
use ciallopa_usage::{Record, Recorder, Transport};
use http::Request;
use serde_json::Value;
use std::sync::Arc;
use tower::ServiceExt;

#[tokio::test]
async fn seeded_records_summary_and_observation_contracts() {
    let dir = tempfile::tempdir().unwrap();
    let key = "sk-cpa-0123456789abcdef";
    let mut cfg = Config::parse(&format!("api_keys = [\"{key}\"]")).unwrap();
    cfg.usage.path = dir.path().join("usage.db");
    let rec = Recorder::open(&cfg.usage).unwrap();
    let now = ciallopa_usage::now_ms() - 1000;
    for (age, error) in [
        (5_400_000, Some("connect")),
        (0, None),
        (0, Some("connect")),
    ] {
        let mut row = Record::new(rec.ids.next_at(now - age), Transport::Sse, "responses");
        row.status = if error.is_some() { 502 } else { 200 };
        row.error = error.map(str::to_owned);
        row.err_source = error.map(|_| "network");
        assert!(rec.record(row));
    }
    rec.flush_blocking().unwrap();
    let app = ciallopa::build_app(super::support::proxy_state(
        cfg,
        Arc::new(super::support::FixedStore::new(&[])),
        Arc::new(super::support::RecordingRouter::default()),
        Some(rec.clone()),
    ));
    for (path, auth, status) in [
        ("/usage/v1/summary", false, 401),
        ("/usage/v1/quota", false, 401),
        ("/usage/v1/records/0000000000000001/observation", false, 401),
        ("/usage/v1/records/xyz/observation", true, 400),
        ("/usage/v1/records/0000000000000001/observation", true, 404),
        ("/usage/v1/records?failure_class=bogus", true, 400),
        (
            "/usage/v1/summary?range=1h&group=error,err_source&compare=1&bucket=hour&tz=0",
            true,
            200,
        ),
        (
            "/usage/v1/records?range=1h&failure_class=network",
            true,
            200,
        ),
    ] {
        let mut req = Request::get(path);
        if auth {
            req = req.header("authorization", format!("Bearer {key}"));
        }
        let r = app
            .clone()
            .oneshot(req.body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(r.status().as_u16(), status, "{path}");
        if status != 200 {
            continue;
        }
        let v: Value =
            serde_json::from_slice(&to_bytes(r.into_body(), 1 << 20).await.unwrap()).unwrap();
        if path.starts_with("/usage/v1/summary") {
            assert_eq!(v["summary"]["total"]["count"], 2);
            assert_eq!(
                v["summary"]["total"]["failure_classes"],
                serde_json::json!({"network":1})
            );
            let group = v["summary"]["groups"]
                .as_array()
                .unwrap()
                .iter()
                .find(|g| g["group"]["error"] == "connect")
                .unwrap();
            assert_eq!(group["group"]["err_source"], "network");
            assert_eq!(v["previous"]["total"]["count"], 1);
            assert_eq!(
                v["previous"]["to"].as_i64().unwrap() - v["previous"]["from"].as_i64().unwrap(),
                3_600_000
            );
        } else {
            assert_eq!(v["total"], 1);
            assert_eq!(v["records"][0]["failure_class"], "network");
        }
    }
    rec.shutdown();
}
