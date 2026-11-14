use super::*;

#[test]
fn migrates_v4_db_and_roundtrips_v5_fields() {
    let d = tempfile::tempdir().unwrap();
    let p = d.path().join("u.db");
    let old_id = (now_ms() - 5_000) << SEQ_BITS;
    {
        let mut c = rusqlite::Connection::open(&p).unwrap();
        store::migrate(&mut c).unwrap();
        c.execute_batch(&format!(
            "ALTER TABLE requests DROP COLUMN req_tier;
             ALTER TABLE requests DROP COLUMN resp_model;
             ALTER TABLE requests DROP COLUMN resp_effort;
             ALTER TABLE requests DROP COLUMN incomplete;
             ALTER TABLE requests DROP COLUMN err_source;
             ALTER TABLE requests DROP COLUMN up_status;
             ALTER TABLE error_detail DROP COLUMN up_req_id;
             ALTER TABLE error_detail DROP COLUMN attempts;
             INSERT INTO labels VALUES(1,'old-acct'),(2,'gpt-5.5'),(3,'default');
             INSERT INTO requests(id,account,model,transport,status,retries,switches,duration_ms,out_tok,tier)
               VALUES({old_id},1,2,1,200,0,0,5,7,3);
             PRAGMA user_version=4;"
        ))
        .unwrap();
    }
    let r = Recorder::open_as(
        &UsageConfig {
            path: p.clone(),
            ..UsageConfig::default()
        },
        "h",
    )
    .unwrap();
    let mut x = Record::new(r.ids.next_at(now_ms()), Transport::Sse, "responses");
    x.status = 200;
    x.model = Some("gpt-5.5".into());
    x.req_tier = Some("priority".into());
    x.service_tier = Some("default".into());
    x.resp_model = Some("gpt-5.5-2026-09-30".into());
    x.resp_effort = Some("medium".into());
    x.incomplete = Some("max_output_tokens".into());
    assert!(r.record(x));
    r.flush_blocking().unwrap();
    let c = store::open_ro(&p).unwrap();
    let v: i64 = c
        .query_row("PRAGMA user_version", [], |x| x.get(0))
        .unwrap();
    assert_eq!(v, store::SCHEMA_VERSION);
    let rows = store::recent(
        &c,
        &RecentQuery {
            limit: 10,
            ..Default::default()
        },
        &PriceTable::default(),
    )
    .unwrap();
    assert_eq!(rows.len(), 2);
    let (new, old) = (&rows[0], &rows[1]);
    assert_eq!(new.incomplete.as_deref(), Some("max_output_tokens"));
    assert_eq!(old.incomplete, None);
    assert!(
        ciallopa_core::Aliaser::looks_like_alias(old.account.as_deref().unwrap_or("")),
        "v12 renames account labels: {:?}",
        old.account
    );
    assert_eq!(old.service_tier.as_deref(), Some("default"));
    assert_eq!(old.requested_service_tier, None);
    assert_eq!(old.response_model, None);
    assert_eq!(old.model_mismatch, None);
    assert_eq!(new.requested_service_tier.as_deref(), Some("priority"));
    assert_eq!(new.service_tier.as_deref(), Some("default"));
    assert_eq!(new.response_model.as_deref(), Some("gpt-5.5-2026-09-30"));
    assert_eq!(new.response_effort.as_deref(), Some("medium"));
    assert_eq!(new.model_mismatch, Some(false));
    let q = |tier: Option<&str>, downgraded| RecentQuery {
        limit: 10,
        tier: tier.map(Into::into),
        downgraded,
        ..Default::default()
    };
    let cnt = |q| store::recent_count(&c, &q).unwrap();
    assert_eq!(cnt(q(None, true)), 1);
    assert_eq!(cnt(q(Some("standard"), false)), 2);
    assert_eq!(cnt(q(Some("fast"), false)), 0);
    let page = store::recent(
        &c,
        &RecentQuery {
            offset: 1,
            ..q(None, false)
        },
        &PriceTable::default(),
    )
    .unwrap();
    assert_eq!(page.len(), 1);
    assert!(ciallopa_core::Aliaser::looks_like_alias(
        page[0].account.as_deref().unwrap_or("")
    ));
}

#[test]
fn migrates_v1_db() {
    let d = tempfile::tempdir().unwrap();
    let p = d.path().join("u.db");
    let old_id = (now_ms() - 5_000) << SEQ_BITS;
    {
        let c = rusqlite::Connection::open(&p).unwrap();
        c.execute_batch(&format!(
            "CREATE TABLE labels(id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE);
             CREATE TABLE requests(id INTEGER PRIMARY KEY, account INTEGER, first_account INTEGER,
               model INTEGER, effort INTEGER, transport INTEGER NOT NULL, endpoint INTEGER,
               status INTEGER NOT NULL, error INTEGER, retries INTEGER NOT NULL,
               switches INTEGER NOT NULL, ttft_ms INTEGER, duration_ms INTEGER NOT NULL,
               in_tok INTEGER, out_tok INTEGER, cached_tok INTEGER, reasoning_tok INTEGER);
             INSERT INTO labels VALUES(1,'old-acct');
             INSERT INTO requests(id,account,transport,status,retries,switches,duration_ms,out_tok)
               VALUES({old_id},1,1,200,0,0,5,7);
             PRAGMA user_version=1;"
        ))
        .unwrap();
    }
    let r = Recorder::open_as(
        &UsageConfig {
            path: p.clone(),
            ..UsageConfig::default()
        },
        "h",
    )
    .unwrap();
    qrec(&r, now_ms(), "new", "k", 1, 1.0, now_ms() / 1000 + 60);
    r.flush_blocking().unwrap();
    let c = store::open_ro(&p).unwrap();
    let v: i64 = c
        .query_row("PRAGMA user_version", [], |x| x.get(0))
        .unwrap();
    assert_eq!(v, store::SCHEMA_VERSION);
    let n: i64 = c
        .query_row("SELECT count(*) FROM requests", [], |x| x.get(0))
        .unwrap();
    assert_eq!(n, 2, "old rows kept");
    let rows = store::recent(
        &c,
        &RecentQuery {
            limit: 10,
            ..Default::default()
        },
        &PriceTable::default(),
    )
    .unwrap();
    assert!(ciallopa_core::Aliaser::looks_like_alias(
        rows[1].account.as_deref().unwrap_or("")
    ));
    assert_eq!(rows[1].key, None);
    r.shutdown();
}

#[tokio::test]
async fn migrate_v13_adds_observation_table() {
    let d = tempfile::tempdir().unwrap();
    let c = cfg(d.path());
    let rid = (now_ms() - 5_000) << SEQ_BITS;
    {
        let mut db = rusqlite::Connection::open(&c.path).unwrap();
        store::migrate(&mut db).unwrap();
        db.execute_batch(&format!(
            "DROP TABLE request_obs;
             INSERT INTO requests(id, transport, status, retries, switches, duration_ms)
               VALUES({rid}, 1, 200, 0, 0, 10);
             PRAGMA user_version = 13;"
        ))
        .unwrap();
    }
    let r = Recorder::open(&c).unwrap();
    let db = rusqlite::Connection::open(&c.path).unwrap();
    let v: i64 = db
        .query_row("PRAGMA user_version", [], |x| x.get(0))
        .unwrap();
    assert_eq!(v, store::SCHEMA_VERSION);
    assert!(r
        .observation(&format!("{rid:016x}"))
        .await
        .unwrap()
        .is_none());
    let n: i64 = db
        .query_row("SELECT COUNT(*) FROM requests", [], |x| x.get(0))
        .unwrap();
    assert_eq!(n, 1);
    r.shutdown();
}

#[tokio::test]
async fn alias_key_stable_across_reopen_and_labels_store_aliases() {
    let d = tempfile::tempdir().unwrap();
    let path = d.path().join("u.db");
    let cfg = ciallopa_core::config::UsageConfig {
        path: path.clone(),
        ..Default::default()
    };
    let id = "codex-dead-user@example.com-plus";
    let r1 = Recorder::open_as(&cfg, "i1").unwrap();
    let al1 = r1.load_or_create_aliaser().unwrap();
    let alias = al1.alias(id);
    assert!(alias.starts_with("acct_"));
    assert!(!alias.contains('@'));
    let mut rec = Record::new(r1.ids.next(), Transport::Sse, "responses");
    rec.account = Some(id.into());
    rec.model = Some("m".into());
    rec.status = 200;
    rec.duration_ms = 1;
    // record() aliases before enqueue
    assert!(r1.record(rec));
    r1.flush_blocking().unwrap();
    let key1 = r1.kv_get("alias_key").unwrap().unwrap();
    r1.shutdown();

    let r2 = Recorder::open_as(&cfg, "i1").unwrap();
    let al2 = r2.load_or_create_aliaser().unwrap();
    assert_eq!(al2.alias(id), alias);
    assert_eq!(r2.kv_get("alias_key").unwrap().unwrap(), key1);
    let c = store::open_ro(&path).unwrap();
    let name: String = c
        .query_row(
            "SELECT name FROM labels WHERE name LIKE 'acct_%' LIMIT 1",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(name, alias);
    // Raw email must not appear in labels.
    let n: i64 = c
        .query_row(
            "SELECT COUNT(*) FROM labels WHERE name LIKE '%@%'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(n, 0);
    r2.shutdown();
}

/// Build a v11-shaped DB with email-bearing account labels and a juicy error body,
/// migrate to v12, then assert no `@` remains anywhere in the DB file bytes.
#[test]
fn migrate_v11_db_has_no_emails() {
    let d = tempfile::tempdir().unwrap();
    let p = d.path().join("u.db");
    let rid = (now_ms() - 5_000) << SEQ_BITS;
    {
        let mut c = rusqlite::Connection::open(&p).unwrap();
        // Create current schema then pin user_version back to 11 with raw labels.
        store::migrate(&mut c).unwrap();
        // Force v11 marker and inject sensitive rows as a pre-v12 DB would look.
        c.execute_batch(&format!(
            "DELETE FROM requests; DELETE FROM error_detail; DELETE FROM reset_consume;
             DELETE FROM quota_obs; DELETE FROM refusals; DELETE FROM labels;
             INSERT INTO labels(id, name) VALUES
               (1, 'codex-x-alice@example.com-plus'),
               (2, 'gpt-5.5'),
               (3, 'Mozilla/5.0 very-long-user-agent-string-that-should-be-clipped-beyond-sixty-chars-ABCDEF');
             INSERT INTO requests(id, account, model, transport, status, error, retries, switches, duration_ms)
               VALUES({rid}, 1, 2, 1, 500, 2, 0, 0, 10);
             INSERT INTO error_detail(id, message, body, body_len, truncated, headers, attempts)
               VALUES({rid},
                 'upstream failed for alice@example.com',
                 '{{ \"type\":\"response.failed\", \"response\": {{ \"id\":\"resp_1\", \"status\":\"failed\", \"error\": {{ \"message\":\"boom\", \"code\":\"x\" }}, \"instructions\":\"SECRET PROMPT\", \"output\":[{{ \"content\":\"hi\" }}], \"model\":\"gpt-5.5\" }} }}',
                 200, 0, '{{}}',
                 '[{{ \"account\": \"codex-x-alice@example.com-plus\", \"status\": 500, \"body_snippet\": \"{{\\\"instructions\\\":\\\"SECRET\\\",\\\"error\\\":{{\\\"message\\\":\\\"x\\\"}}}}\" }}]');
             INSERT INTO reset_consume(account, redeem_request_id, started_ms, status)
               VALUES('codex-x-alice@example.com-plus', 'rr-1', 1, 'done');
             INSERT INTO refusals(minute, reason, transport, key, route, ua, model, account, instance, lim,
                                  count, first_ms, last_ms, max_inflight, max_ws, max_buffered)
               VALUES(1, 1, 3, 0, 0, 3, 0, 1, 0, 0, 1, 1, 1, 0, 0, 0);
             DELETE FROM kv WHERE k = 'alias_key';
             PRAGMA user_version = 11;"
        ))
        .unwrap();
    }
    // Migrate via open_rw.
    {
        let _c = store::open_rw(&p).unwrap();
    }
    // Column-level check (raw `@` also appears in SQLite page headers as 0x40).
    {
        let c = rusqlite::Connection::open(&p).unwrap();
        let mut dirty = Vec::new();
        for (sql,) in [
            ("SELECT name FROM labels",),
            ("SELECT account, COALESCE(error,''), COALESCE(credit_id,'') FROM reset_consume",),
            ("SELECT COALESCE(message,''), COALESCE(body,''), COALESCE(attempts,''), COALESCE(headers,'') FROM error_detail",),
        ] {
            let mut st = c.prepare(sql).unwrap();
            let cols = st.column_count();
            for row in st
                .query_map([], |r| {
                    let mut s = String::new();
                    for i in 0..cols {
                        let v: String = r.get::<_, Option<String>>(i).ok().flatten().unwrap_or_default();
                        s.push_str(&v);
                        s.push(' ');
                    }
                    Ok(s)
                })
                .unwrap()
            {
                let s = row.unwrap();
                if s.contains('@') || s.contains("SECRET") {
                    dirty.push(s);
                }
            }
        }
        assert!(
            dirty.is_empty(),
            "sensitive text still in columns: {dirty:?}"
        );
    }
    let bytes = std::fs::read(&p).unwrap();
    let text = String::from_utf8_lossy(&bytes);
    assert!(
        !text.contains("example.com") && !text.contains("SECRET PROMPT"),
        "fixture secrets still in db file bytes"
    );
    // Labels are aliases; UA was clipped.
    let c = rusqlite::Connection::open(&p).unwrap();
    let v: i64 = c
        .query_row("PRAGMA user_version", [], |r| r.get(0))
        .unwrap();
    assert_eq!(v, store::SCHEMA_VERSION);
    let names: Vec<String> = {
        let mut st = c.prepare("SELECT name FROM labels ORDER BY id").unwrap();
        st.query_map([], |r| r.get(0))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap()
    };
    let acct = names
        .iter()
        .find(|n| ciallopa_core::Aliaser::looks_like_alias(n))
        .expect("aliased account label");
    assert!(acct.starts_with("acct_"));
    let ua = names
        .iter()
        .find(|n| n.starts_with("Mozilla"))
        .expect("ua label");
    assert!(ua.chars().count() <= 60, "ua len {}", ua.chars().count());
    let rc_acct: String = c
        .query_row("SELECT account FROM reset_consume LIMIT 1", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert!(ciallopa_core::Aliaser::looks_like_alias(&rc_acct));
}

#[test]
fn migrate_v12_to_v13_only_adds_thread_index() {
    let d = tempfile::tempdir().unwrap();
    let p = d.path().join("u.db");
    let mut c = rusqlite::Connection::open(&p).unwrap();
    store::migrate(&mut c).unwrap();
    // A v12 DB: no thread index; a raw label shows the v12 rewrite does not run again.
    c.execute_batch(
        "DROP INDEX requests_thread;
         INSERT INTO labels(id, name) VALUES (1, 'codex-x-bob@example.com-plus');
         PRAGMA user_version = 12;",
    )
    .unwrap();
    store::migrate(&mut c).unwrap();
    let v: i64 = c
        .query_row("PRAGMA user_version", [], |r| r.get(0))
        .unwrap();
    assert_eq!(v, store::SCHEMA_VERSION);
    let idx: i64 = c
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name='requests_thread'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(idx, 1);
    let name: String = c
        .query_row("SELECT name FROM labels WHERE id = 1", [], |r| r.get(0))
        .unwrap();
    assert_eq!(name, "codex-x-bob@example.com-plus");
}

#[test]
fn migrate_v12_is_idempotent() {
    let d = tempfile::tempdir().unwrap();
    let p = d.path().join("u.db");
    {
        let mut c = rusqlite::Connection::open(&p).unwrap();
        store::migrate(&mut c).unwrap();
        c.execute_batch(
            "INSERT INTO labels(id, name) VALUES (1, 'codex-x-bob@example.com-plus');
             INSERT INTO requests(id, account, transport, status, retries, switches, duration_ms)
               VALUES(1, 1, 1, 200, 0, 0, 1);
             DELETE FROM kv WHERE k = 'alias_key';
             PRAGMA user_version = 11;",
        )
        .unwrap();
    }
    {
        let mut c = rusqlite::Connection::open(&p).unwrap();
        store::migrate(&mut c).unwrap();
    }
    let name1: String = {
        let c = rusqlite::Connection::open(&p).unwrap();
        c.query_row("SELECT name FROM labels WHERE id = 1", [], |r| r.get(0))
            .unwrap()
    };
    let key1: String = {
        let c = rusqlite::Connection::open(&p).unwrap();
        c.query_row("SELECT v FROM kv WHERE k = 'alias_key'", [], |r| r.get(0))
            .unwrap()
    };
    // Run migrate again (v already 12 → early return; also exercise rewrite idempotency
    // by forcing rewrite path).
    {
        let mut c = rusqlite::Connection::open(&p).unwrap();
        // Force rewrite path again.
        c.execute_batch("PRAGMA user_version = 11;").unwrap();
        store::migrate(&mut c).unwrap();
    }
    let c = rusqlite::Connection::open(&p).unwrap();
    let name2: String = c
        .query_row("SELECT name FROM labels WHERE id = 1", [], |r| r.get(0))
        .unwrap();
    let key2: String = c
        .query_row("SELECT v FROM kv WHERE k = 'alias_key'", [], |r| r.get(0))
        .unwrap();
    assert_eq!(name1, name2);
    assert_eq!(key1, key2);
    assert!(ciallopa_core::Aliaser::looks_like_alias(&name1));
}

/// A v12 DB with only aliases still opens and queries without error (stands in for
/// "old binary reading v12": labels look like unknown accounts, no crash).
#[test]
fn v12_db_with_aliases_queries_cleanly() {
    let d = tempfile::tempdir().unwrap();
    let p = d.path().join("u.db");
    let rid = (now_ms() - 1_000) << SEQ_BITS;
    {
        let mut c = rusqlite::Connection::open(&p).unwrap();
        store::migrate(&mut c).unwrap();
        c.execute_batch(&format!(
            "INSERT INTO labels(id, name) VALUES (1, 'acct_deadbeef'), (2, 'gpt-5.5');
             INSERT INTO requests(id, account, model, transport, status, retries, switches, duration_ms)
               VALUES({rid}, 1, 2, 1, 200, 0, 0, 5);"
        ))
        .unwrap();
    }
    let c = store::open_ro(&p).unwrap();
    let rows = store::recent(
        &c,
        &RecentQuery {
            limit: 5,
            ..Default::default()
        },
        &PriceTable::default(),
    )
    .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].account.as_deref(), Some("acct_deadbeef"));
    assert_eq!(rows[0].model.as_deref(), Some("gpt-5.5"));
}
