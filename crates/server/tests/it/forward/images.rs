use super::scaffold::*;
use super::*;

#[tokio::test]
async fn images_wrap_and_direct() {
    let e = env(300).await;
    let r = e
        .post("/v1/images/generations")
        .header("x-test-mode", "image")
        .json(&serde_json::json!({"prompt":"cat","size":"1024x1024"}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let v: serde_json::Value = r.json().await.unwrap();
    assert_eq!(v["data"][0]["b64_json"], "SU1H");
    assert_eq!(v["created"], 7);
    assert_eq!(v["usage"]["images"], 1);
    let sent: serde_json::Value =
        serde_json::from_slice(&e.mock.seen.lock().unwrap()[0].1).unwrap();
    assert_eq!(sent["tools"][0]["type"], "image_generation");
    assert_eq!(sent["tools"][0]["model"], "gpt-image-2");
    assert_eq!(sent["model"], "gpt-6.1-sol");
    assert!(
        !e.mock.seen.lock().unwrap()[0]
            .0
            .iter()
            .any(|(k, _)| k == "content-encoding"),
        "images wrap is never compressed"
    );

    // streaming wrap
    let r = e
        .post("/v1/images/generations")
        .header("x-test-mode", "image")
        .json(&serde_json::json!({"prompt":"cat","stream":true,"model":"gpt-image-1"}))
        .send()
        .await
        .unwrap();
    let t = r.text().await.unwrap();
    assert!(
        t.contains("event: image_generation.partial_image")
            && t.contains("event: image_generation.completed"),
        "{t}"
    );

    // direct (gpt-image-2) multipart edit → JSON
    let mp = "--XB\r\nContent-Disposition: form-data; name=\"model\"\r\n\r\ngpt-image-2\r\n--XB\r\nContent-Disposition: form-data; name=\"prompt\"\r\n\r\nedit it\r\n--XB\r\nContent-Disposition: form-data; name=\"image[]\"; filename=\"a.png\"\r\n\r\n\x7fPNGxx\r\n--XB--\r\n";
    let mp = mp.replace('\x7f', "\u{89}");
    let r = e
        .post("/v1/images/edits")
        .header("content-type", "multipart/form-data; boundary=XB")
        .body(mp)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let seen = e.mock.seen.lock().unwrap();
    let j: serde_json::Value = serde_json::from_slice(&seen.last().unwrap().1).unwrap();
    assert_eq!(j["model"], "gpt-image-2");
    assert!(j["images"][0]["image_url"]
        .as_str()
        .unwrap()
        .starts_with("data:"));
    assert_eq!(j["prompt"], "edit it");
    assert!(seen
        .last()
        .unwrap()
        .0
        .iter()
        .any(|(k, v)| k == "originator" && v == "codex_exec"));
}

#[tokio::test]
async fn image_records_wait_for_body_and_direct_sse_is_not_responses() {
    let dir = tempfile::tempdir().unwrap();
    let rec = usage_recorder(dir.path());
    let e = env_usage(300, Some(rec.clone())).await;
    let r = e
        .post("/v1/images/generations")
        .header("x-test-mode", "image_body_late_error")
        .json(&serde_json::json!({"model":"gpt-image-2","prompt":"cat"}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    assert_eq!(
        rec.stats().accepted,
        0,
        "headers alone must not record success"
    );
    e.mock.release.notify_one();
    assert!(r.bytes().await.is_err());
    let r = e
        .post("/v1/images/generations")
        .header("x-test-mode", "image_direct_sse")
        .json(&serde_json::json!({"model":"gpt-image-2","prompt":"cat","stream":true}))
        .send()
        .await
        .unwrap();
    assert_eq!(
        r.text().await.unwrap(),
        IMAGE_DIRECT_SSE,
        "no fabricated Responses failure at EOF"
    );
    let rows = tokio::time::timeout(Duration::from_secs(5), wait_rows(&e, 2))
        .await
        .unwrap();
    let failed = rows
        .iter()
        .find(|r| r["error"] == "stream_read_error")
        .unwrap();
    assert_eq!(failed["status"], 200);
    assert_eq!(failed["err_source"], "network");
    assert_eq!(failed["account"], "a");
    let ok = rows.iter().find(|r| r["error"].is_null()).unwrap();
    assert_eq!(ok["transport"], "sse");
    for r in rows {
        assert!(r["ttft_ms"].is_null());
        assert!(r["input_tokens"].is_null() && r["output_tokens"].is_null());
    }
    rec.shutdown();
}

#[tokio::test]
async fn image_direct_errors_are_observed_without_changing_bytes_or_body_lifetime() {
    let dir = tempfile::tempdir().unwrap();
    let rec = usage_recorder(dir.path());
    let e = env_usage(300, Some(rec.clone())).await;
    for (i, mode) in [
        "image_error_event",
        "image_error_type",
        "image_error_after_large",
        "image_error_words",
    ]
    .into_iter()
    .enumerate()
    {
        let expected = image_event_fixture(mode);
        let r = e
            .post("/v1/images/generations")
            .header("x-test-mode", mode)
            .json(&serde_json::json!({"model":"gpt-image-2","prompt":"cat","stream":true}))
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 200);
        let mut body = r.bytes_stream();
        let mut got = Vec::new();
        while got.len() < expected.len() {
            got.extend_from_slice(
                &tokio::time::timeout(Duration::from_secs(3), body.next())
                    .await
                    .unwrap()
                    .unwrap()
                    .unwrap(),
            );
        }
        assert_eq!(
            got,
            expected.as_bytes(),
            "original Images bytes must be delivered before EOF"
        );
        assert_eq!(
            rec.stats().accepted,
            i as u64,
            "observing an error must not finalize a pending Body"
        );
        e.mock.release.notify_one();
        while let Some(chunk) = body.next().await {
            got.extend_from_slice(&chunk.unwrap());
        }
        assert_eq!(
            got,
            expected.as_bytes(),
            "must not append a Responses error event"
        );
        let rows = tokio::time::timeout(Duration::from_secs(3), wait_rows(&e, i + 1))
            .await
            .unwrap();
        let row = &rows[0];
        assert_eq!(row["status"], 200);
        assert_eq!(row["transport"], "sse");
        assert_eq!(row["account"], "a");
        assert!(
            row["ttft_ms"].is_null()
                && row["input_tokens"].is_null()
                && row["output_tokens"].is_null()
        );
        if mode == "image_error_words" {
            assert!(
                row["error"].is_null(),
                "nested or literal error text must not classify the event"
            );
        } else {
            assert_eq!(row["error"], "upstream_error_event");
            assert_eq!(row["err_source"], "stream");
            let detail: serde_json::Value =
                detail_of(&e, row["request_id"].as_str().unwrap(), true)
                    .await
                    .json()
                    .await
                    .unwrap();
            assert_eq!(detail["upstream_request_id"], "images-fixture-request");
            assert!(detail["message"]
                .as_str()
                .unwrap()
                .contains("fixture image failure"));
            assert!(!detail["body"].as_str().unwrap().contains(&"A".repeat(100)));
        }
    }
    assert_eq!(
        e.mock.hits.load(Ordering::SeqCst),
        4,
        "no replay of image failures"
    );
    assert!(e
        .outcomes()
        .iter()
        .all(|(_, o)| matches!(o, Outcome::Success { .. })));
    rec.shutdown();
}

#[tokio::test]
async fn image_wrapped_results_record_success_failure_and_truncated_eof() {
    let dir = tempfile::tempdir().unwrap();
    let rec = usage_recorder(dir.path());
    let e = env_usage(300, Some(rec.clone())).await;
    for (mode, stream) in [
        ("image", false),
        ("image", true),
        ("image_failed", false),
        ("image_failed", true),
        ("image_truncated", true),
    ] {
        let r = e
            .post("/v1/images/generations")
            .header("x-test-mode", mode)
            .json(&serde_json::json!({"model":"gpt-image-1","prompt":"cat","stream":stream}))
            .send()
            .await
            .unwrap();
        assert_eq!(
            r.status().as_u16(),
            if mode == "image_failed" && !stream {
                502
            } else {
                200
            }
        );
        let body = r.text().await.unwrap();
        assert!(if mode == "image" {
            body.contains("SU1H")
        } else {
            body.contains("error")
        });
    }
    let rows = tokio::time::timeout(Duration::from_secs(5), wait_rows(&e, 5))
        .await
        .unwrap();
    assert_eq!(rows.iter().filter(|r| r["error"].is_null()).count(), 2);
    assert_eq!(
        rows.iter().filter(|r| r["error"] == "image_failed").count(),
        2
    );
    assert_eq!(
        rows.iter()
            .filter(|r| r["error"] == "stream_truncated")
            .count(),
        1
    );
    assert_eq!(rows.iter().filter(|r| r["transport"] == "sse").count(), 3);
    for r in rows {
        assert_eq!(r["account"], "a");
        assert_eq!(r["model"], "gpt-image-1");
        assert!(r["ttft_ms"].is_null());
        assert!(
            r["input_tokens"].is_null() && r["output_tokens"].is_null(),
            "internal text model usage is not image-token usage: {r}"
        );
        if !r["error"].is_null() {
            assert_eq!(r["err_source"], "stream");
        }
        if r["error"] == "image_failed" {
            let detail: serde_json::Value = detail_of(&e, r["request_id"].as_str().unwrap(), true)
                .await
                .json()
                .await
                .unwrap();
            assert_eq!(detail["headers"]["x-request-id"], "image-upstream-id");
        }
    }
    rec.shutdown();
}

#[tokio::test]
async fn empty_http_and_image_bodies_complete_without_cancellation() {
    let dir = tempfile::tempdir().unwrap();
    let rec = usage_recorder(dir.path());
    let e = env_usage(300, Some(rec.clone())).await;
    for path in ["/v1/images/generations", "/v1/responses"] {
        for mode in ["empty_200", "empty_204"] {
            let r = e
                .post(path)
                .header("x-test-mode", mode)
                .json(&serde_json::json!({"model":"gpt-image-2","stream":false,"input":[],"prompt":"cat"}))
                .send()
                .await
                .unwrap();
            assert_eq!(
                r.status().as_u16(),
                if mode == "empty_204" { 204 } else { 200 }
            );
            assert!(r.bytes().await.unwrap().is_empty());
        }
    }
    let rows = tokio::time::timeout(Duration::from_secs(5), wait_rows(&e, 4))
        .await
        .unwrap();
    assert_eq!(rows.len(), 4);
    for row in rows {
        assert!(
            row["error"].is_null(),
            "empty HTTP framing proves EOF: {row}"
        );
        assert_eq!(row["transport"], "http");
    }
    rec.shutdown();
}

#[tokio::test]
async fn image_response_incomplete_is_recorded_not_truncated() {
    let d = tempfile::tempdir().unwrap();
    let e = env_usage(300, Some(usage_recorder(d.path()))).await;
    // An incomplete response that still carries an image returns it.
    let r = e
        .post("/v1/images/generations")
        .header("x-test-mode", "image_incomplete")
        .json(&serde_json::json!({"prompt":"cat"}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let v: serde_json::Value = r.json().await.unwrap();
    assert_eq!(v["data"][0]["b64_json"], "SU1H");
    let rows = wait_rows(&e, 1).await;
    assert!(rows[0]["error"].is_null(), "{}", rows[0]);
    assert_eq!(rows[0]["incomplete"], "max_output_tokens", "{}", rows[0]);
    // Filtered without an image: the client gets an error, logged as content_filter.
    for stream in [false, true] {
        let r = e
            .post("/v1/images/generations")
            .header("x-test-mode", "image_content_filter")
            .json(&serde_json::json!({"prompt":"cat","stream":stream}))
            .send()
            .await
            .unwrap();
        if !stream {
            assert_eq!(r.status(), 502);
        }
        r.bytes().await.unwrap();
    }
    let rows = wait_rows(&e, 3).await;
    for row in &rows[..2] {
        assert_eq!(row["error"], "content_filter", "{row}");
        assert_eq!(row["err_source"], "stream", "{row}");
        assert_eq!(row["incomplete"], "content_filter", "{row}");
    }
}

#[tokio::test]
async fn image_non_stream_truncation_is_bad_gateway() {
    for (mode, code, source) in [
        ("image_truncated", "stream_truncated", "stream"),
        ("image_reset", "stream_read_error", "network"),
    ] {
        let d = tempfile::tempdir().unwrap();
        let e = env_usage(300, Some(usage_recorder(d.path()))).await;
        let r = e
            .post("/v1/images/generations")
            .header("x-test-mode", mode)
            .json(&serde_json::json!({"model":"gpt-image-1","prompt":"cat"}))
            .send()
            .await
            .unwrap();
        assert_eq!(r.status().as_u16(), 502, "{mode}");
        let rows = wait_rows(&e, 1).await;
        assert_eq!(rows[0]["error"], code, "{mode}: {}", rows[0]);
        assert_eq!(rows[0]["err_source"], source, "{mode}: {}", rows[0]);
    }
}
