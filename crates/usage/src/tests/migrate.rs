use super::*;

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
    let c = crate::schema::open_ro(&path).unwrap();
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
        create_fixture(&mut c, 11);
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
        let _c = crate::schema::open_rw(&p).unwrap();
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
    assert_eq!(v, crate::schema::SCHEMA_VERSION);
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
fn migrate_v12_is_idempotent() {
    let d = tempfile::tempdir().unwrap();
    let p = d.path().join("u.db");
    {
        let mut c = rusqlite::Connection::open(&p).unwrap();
        create_fixture(&mut c, 11);
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
        crate::schema::migrate(&mut c).unwrap();
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
    // Rewriting aliases a second time must preserve both aliases and the stored key.
    {
        let c = rusqlite::Connection::open(&p).unwrap();
        crate::migrate_v12::rewrite(&c).unwrap();
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

// Frozen v15 DDL is independent of the production LATEST and migration steps.
fn create_fixture(c: &mut rusqlite::Connection, version: i64) {
    let request_columns = [
        (
            2,
            &[
                "key", "instance", "plan", "p_pct", "p_reset", "s_pct", "s_reset",
            ][..],
        ),
        (3, &["tier", "cw_tok", "thread", "turn"][..]),
        (5, &["req_tier", "resp_model", "resp_effort"][..]),
        (7, &["incomplete"][..]),
        (9, &["err_source", "up_status"][..]),
    ];
    let ddl: String = include_str!("fixtures/v15.sql")
        .lines()
        .map(|l| format!("{}\n", l.split("--").next().unwrap()))
        .collect();
    for statement in ddl.split(';') {
        let mut lines: Vec<String> = statement
            .lines()
            .filter(|l| !l.trim().starts_with("--"))
            .map(str::to_string)
            .collect();
        let sql = lines.join("\n");
        if sql.trim().is_empty() {
            continue;
        }
        let introduced = if sql.contains("requests_thread") {
            13
        } else if sql.contains("quota_reset_events") || sql.contains("quota_cycle_estimates") {
            15
        } else if sql.contains("request_obs") {
            14
        } else if sql.contains("occupancy") {
            11
        } else if sql.contains("refusals") {
            10
        } else if sql.contains("kv(") {
            8
        } else if sql.contains("reset_consume") {
            6
        } else if sql.contains("error_detail") {
            4
        } else if sql.contains("quota_obs") {
            2
        } else {
            1
        };
        if version < introduced {
            continue;
        }
        if sql.contains("CREATE TABLE IF NOT EXISTS requests(") {
            lines.retain(|line| {
                let column = line.split_whitespace().next().unwrap_or("");
                !request_columns
                    .iter()
                    .any(|(v, cols)| version < *v && cols.contains(&column))
            });
        }
        if sql.contains("CREATE TABLE IF NOT EXISTS error_detail(") && version < 9 {
            lines.retain(|line| {
                !matches!(
                    line.split_whitespace().next(),
                    Some("up_req_id" | "attempts")
                )
            });
        }
        if sql.contains("CREATE TABLE IF NOT EXISTS quota_obs(") && version < 15 {
            lines = lines
                .into_iter()
                .map(|l| l.replace(", cards INTEGER", ""))
                .collect();
        }
        // Removing trailing columns leaves their predecessor's comma before the closing `)`.
        if sql.contains("CREATE TABLE") && !sql.contains("WITHOUT ROWID") {
            let end = lines.iter().rposition(|l| l.trim() == ")").unwrap();
            let previous = lines[..end]
                .iter()
                .rposition(|l| !l.trim().is_empty())
                .unwrap();
            let column = lines[previous]
                .split("--")
                .next()
                .unwrap()
                .trim_end()
                .trim_end_matches(',');
            lines[previous] = column.to_string();
        }
        c.execute_batch(&lines.join("\n")).unwrap();
    }
    if version >= 12 {
        crate::migrate_v12::load_or_create_aliaser(c).unwrap();
    }
    if version >= 16 {
        // Freeze the v16 additions on top of v15, independently of LATEST.
        c.execute_batch(
            "CREATE INDEX requests_account ON requests(account,id);
             CREATE TABLE threads(id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE);
             INSERT INTO threads SELECT DISTINCT l.id,l.name FROM labels l JOIN requests r ON r.thread=l.id;
             UPDATE reset_consume SET code=lower(code);",
        )
        .unwrap();
    }
    if version >= 17 {
        c.execute_batch(
            "DROP INDEX requests_account; CREATE INDEX requests_account ON requests(account)",
        )
        .unwrap();
    }
    c.pragma_update(None, "user_version", version).unwrap();
}

fn fixture(version: i64) -> rusqlite::Connection {
    let mut c = rusqlite::Connection::open_in_memory().unwrap();
    create_fixture(&mut c, version);
    c.execute_batch("INSERT INTO labels VALUES(1,'acct_01234567'),(2,'gpt-5.5');
        INSERT INTO requests(id,account,model,transport,status,retries,switches,duration_ms,out_tok) VALUES(1048576,1,2,1,200,0,0,5,7);").unwrap();
    c
}

#[test]
fn historical_schemas_upgrade_and_roundtrip() {
    for version in [1, 4, 11, 12, 13, 14, 15, 16] {
        let mut c = fixture(version);
        crate::schema::migrate(&mut c).unwrap();
        assert_eq!(
            c.pragma_query_value::<i64, _>(None, "user_version", |r| r.get(0))
                .unwrap(),
            crate::schema::SCHEMA_VERSION,
            "v{version}"
        );
        for table in [
            "request_obs",
            "threads",
            "quota_reset_events",
            "quota_cycle_estimates",
            "reset_consume",
            "quota_est_epochs",
        ] {
            assert_eq!(
                c.query_row(
                    "SELECT count(*) FROM sqlite_master WHERE name=?1",
                    [table],
                    |r| r.get::<_, i64>(0)
                )
                .unwrap(),
                1,
                "v{version} {table}"
            );
        }
        assert_eq!(
            c.query_row(
                "SELECT count(*) FROM pragma_table_info('quota_obs') WHERE name='cards'",
                [],
                |r| r.get::<_, i64>(0)
            )
            .unwrap(),
            1
        );
        let rows = crate::records::recent(
            &c,
            &RecentQuery {
                limit: 10,
                ..Default::default()
            },
            &PriceTable::default(),
        )
        .unwrap();
        assert_eq!(rows.len(), 1, "v{version}");
        assert_eq!(rows[0].output_tokens, Some(7));
        assert_eq!(rows[0].model.as_deref(), Some("gpt-5.5"));
        assert!(ciallopa_core::Aliaser::looks_like_alias(
            rows[0].account.as_deref().unwrap()
        ));
        assert_eq!(c.prepare("SELECT req_tier,resp_model,resp_effort,incomplete,err_source,up_status FROM requests").unwrap().column_count(), 6);
        crate::schema::migrate(&mut c).unwrap();
    }
}

#[test]
fn v15_threads_preserve_ids_shared_labels_and_all_persisted_data() {
    let mut c = fixture(15);
    c.execute_batch("INSERT INTO labels VALUES(10,'session-unique'),(11,'session-shared'),(12,'orphan-session');
        UPDATE requests SET thread=10;
        INSERT INTO requests(id,model,thread,transport,status,retries,switches,duration_ms) VALUES(2097152,11,11,2,200,0,0,6);
        INSERT INTO quota_obs(id,account,cards) VALUES(1048576,1,3);
        INSERT INTO error_detail(id,message,body_len,truncated,headers) VALUES(1048576,'kept',0,0,'{}');
        INSERT INTO reset_consume(account,redeem_request_id,started_ms,status,code) VALUES('acct_01234567','rid',1,'superseded','RESET');
        INSERT INTO request_obs(id,up_events) VALUES(1048576,17);
        INSERT INTO quota_reset_events(id,account,window,kind,at_ms,obs_id,cycle_id) VALUES(20,1,'primary','natural',1000,1048576,21);
        INSERT INTO quota_cycle_estimates(id,account,window,started_ms,ended_ms,ended_obs_id,close_kind,capacity_credits,confidence) VALUES(21,1,'primary',0,1000,1048576,'natural',123.5,'high');
        INSERT INTO refusals(minute,reason,transport,key,route,ua,model,account,instance,lim,count,first_ms,last_ms,max_inflight,max_ws,max_buffered) VALUES(60000,1,3,0,0,11,0,1,0,1,9,60000,60001,1,0,0);
        INSERT INTO occupancy(minute,instance,samples,inflight_sum,inflight_peak,ws_sum,ws_peak,buffered_sum,buffered_peak,lim_inflight,lim_ws,lim_buffered) VALUES(60000,0,3,4,5,0,0,0,0,6,0,0);").unwrap();
    let key: String = c
        .query_row("SELECT v FROM kv WHERE k='alias_key'", [], |r| r.get(0))
        .unwrap();
    crate::schema::migrate(&mut c).unwrap();
    assert_eq!(
        c.query_row("SELECT name FROM threads WHERE id=10", [], |r| r
            .get::<_, String>(0))
            .unwrap(),
        "session-unique"
    );
    assert_eq!(
        c.query_row("SELECT count(*) FROM labels WHERE id=10", [], |r| r
            .get::<_, i64>(0))
            .unwrap(),
        0
    );
    assert_eq!(
        c.query_row("SELECT name FROM labels WHERE id=11", [], |r| r
            .get::<_, String>(0))
            .unwrap(),
        "session-shared"
    );
    assert_eq!(
        c.query_row("SELECT cards FROM quota_obs", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        3
    );
    assert_eq!(
        c.query_row("SELECT message FROM error_detail", [], |r| r
            .get::<_, String>(0))
            .unwrap(),
        "kept"
    );
    assert_eq!(
        c.query_row("SELECT code FROM reset_consume", [], |r| r
            .get::<_, String>(0))
            .unwrap(),
        "reset"
    );
    assert_eq!(
        c.query_row("SELECT v FROM kv WHERE k='alias_key'", [], |r| r
            .get::<_, String>(0))
            .unwrap(),
        key
    );
    for (sql, expected) in [
        ("SELECT count(*) FROM labels WHERE id=12", 0),
        ("SELECT up_events FROM request_obs WHERE id=1048576", 17),
        ("SELECT cycle_id FROM quota_reset_events WHERE id=20", 21),
        ("SELECT count FROM refusals WHERE ua=11", 9),
        ("SELECT inflight_peak FROM occupancy", 5),
    ] {
        assert_eq!(
            c.query_row(sql, [], |r| r.get::<_, i64>(0)).unwrap(),
            expected
        );
    }
    assert_eq!(
        c.query_row(
            "SELECT capacity_credits FROM quota_cycle_estimates WHERE id=21",
            [],
            |r| r.get::<_, f64>(0)
        )
        .unwrap(),
        123.5
    );
    let rows = crate::records::recent(
        &c,
        &RecentQuery {
            limit: 10,
            ..Default::default()
        },
        &PriceTable::default(),
    )
    .unwrap();
    assert_eq!(rows[0].thread_id.as_deref(), Some("session-shared"));
    assert_eq!(rows[0].model.as_deref(), Some("session-shared"));
    assert_eq!(rows[1].thread_id.as_deref(), Some("session-unique"));
}

fn assert_account_index(c: &rusqlite::Connection) {
    let properties: (String, i64, i64) = c
        .query_row(
            "SELECT origin,\"unique\",partial FROM pragma_index_list('requests') WHERE name='requests_account'",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .unwrap();
    assert_eq!(properties, ("c".into(), 0, 0));
    let keys: Vec<(String, i64, String)> = c
        .prepare("SELECT name,desc,coll FROM pragma_index_xinfo('requests_account') WHERE key=1")
        .unwrap()
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(keys, vec![("account".into(), 0, "BINARY".into())]);
}

#[test]
fn v17_fresh_and_historical_files_reopen_without_rebuilding() {
    for version in [0, 15, 16] {
        let d = tempfile::tempdir().unwrap();
        let path = d.path().join("u.db");
        if version != 0 {
            let mut c = rusqlite::Connection::open(&path).unwrap();
            create_fixture(&mut c, version);
            c.execute_batch(
                "INSERT INTO labels VALUES(1,'acct_01234567');
                 INSERT INTO requests(id,account,thread,transport,status,retries,switches,duration_ms)
                 VALUES(1048576,1,10,1,200,0,0,5),(1048577,NULL,NULL,1,200,0,0,6);
                 INSERT INTO quota_obs(id,account,cards) VALUES(1048576,1,3);
                 INSERT INTO quota_reset_events(account,window,kind,at_ms,obs_id)
                 VALUES(1,'primary','natural',1,1048576);
                 INSERT INTO quota_cycle_estimates(account,window,started_ms,ended_ms,ended_obs_id,close_kind,capacity_credits,confidence)
                 VALUES(1,'primary',0,1,1048576,'natural',123.5,'high');",
            ).unwrap();
            if version == 15 {
                c.execute_batch("INSERT INTO labels VALUES(10,'session-kept')")
                    .unwrap();
            } else {
                c.execute_batch("INSERT INTO threads VALUES(10,'session-kept')")
                    .unwrap();
            }
            if version == 16 {
                assert_eq!(
                    c.query_row(
                        "SELECT count(*) FROM pragma_index_info('requests_account')",
                        [],
                        |r| r.get::<_, i64>(0)
                    )
                    .unwrap(),
                    2
                );
            }
        }
        let cookie = {
            let c = crate::schema::open_rw(&path).unwrap();
            assert_account_index(&c);
            c.pragma_query_value::<i64, _>(None, "schema_version", |r| r.get(0))
                .unwrap()
        };
        for _ in 0..2 {
            let c = crate::schema::open_rw(&path).unwrap();
            assert_account_index(&c);
            assert_eq!(
                c.pragma_query_value::<i64, _>(None, "user_version", |r| r.get(0))
                    .unwrap(),
                crate::schema::SCHEMA_VERSION
            );
            assert_eq!(
                c.pragma_query_value::<i64, _>(None, "schema_version", |r| r.get(0))
                    .unwrap(),
                cookie
            );
            if version != 0 {
                for (sql, expected) in [
                    ("SELECT count(*) FROM requests", 2),
                    (
                        "SELECT count(*) FROM requests WHERE account IS NULL AND id=1048577",
                        1,
                    ),
                    ("SELECT thread FROM requests WHERE id=1048576", 10),
                    ("SELECT cards FROM quota_obs", 3),
                    ("SELECT obs_id FROM quota_reset_events", 1048576),
                ] {
                    assert_eq!(
                        c.query_row(sql, [], |r| r.get::<_, i64>(0)).unwrap(),
                        expected
                    );
                }
                assert_eq!(
                    c.query_row("SELECT name FROM threads WHERE id=10", [], |r| r
                        .get::<_, String>(0))
                        .unwrap(),
                    "session-kept"
                );
                assert_eq!(
                    c.query_row(
                        "SELECT capacity_credits FROM quota_cycle_estimates",
                        [],
                        |r| r.get::<_, f64>(0)
                    )
                    .unwrap(),
                    123.5
                );
            }
        }
    }
}

#[test]
fn v17_does_not_rebuild_an_existing_target_index() {
    let mut c = fixture(16);
    c.execute_batch(
        "DROP INDEX requests_account; CREATE INDEX requests_account ON requests(account)",
    )
    .unwrap();
    let before: i64 = c
        .pragma_query_value(None, "schema_version", |r| r.get(0))
        .unwrap();
    crate::schema::migrate(&mut c).unwrap();
    assert_eq!(
        c.pragma_query_value::<i64, _>(None, "schema_version", |r| r.get(0))
            .unwrap(),
        // v18 adds the expiry column, v19 four credit columns plus the epoch table and its
        // index; the account index remains unchanged.
        before + 1 + 6
    );
    assert_account_index(&c);

    // In v15, v16's LATEST creates a missing index. The following v17 step
    // must not drop and create it again: that would add two schema changes.
    let mut missing = fixture(15);
    let mut prebuilt = fixture(15);
    prebuilt
        .execute_batch("CREATE INDEX requests_account ON requests(account)")
        .unwrap();
    let cookie = |c: &rusqlite::Connection| {
        c.pragma_query_value::<i64, _>(None, "schema_version", |r| r.get(0))
            .unwrap()
    };
    let (m, w) = (cookie(&missing), cookie(&prebuilt));
    crate::schema::migrate(&mut missing).unwrap();
    crate::schema::migrate(&mut prebuilt).unwrap();
    assert_eq!(cookie(&missing) - m, cookie(&prebuilt) - w + 1);
    assert_account_index(&missing);
}

#[test]
fn v17_failed_replacement_rolls_back_index_and_version() {
    let d = tempfile::tempdir().unwrap();
    let path = d.path().join("u.db");
    let mut c = rusqlite::Connection::open(&path).unwrap();
    create_fixture(&mut c, 16);
    c.busy_timeout(std::time::Duration::ZERO).unwrap();
    // A real read transaction in rollback-journal mode prevents the migration
    // commit. The replacement index and version must roll back together.
    let reader = rusqlite::Connection::open(&path).unwrap();
    reader
        .execute_batch("BEGIN; SELECT * FROM requests;")
        .unwrap();
    let cookie: i64 = c
        .pragma_query_value(None, "schema_version", |r| r.get(0))
        .unwrap();
    let error = crate::schema::migrate(&mut c).unwrap_err();
    assert_eq!(
        error.sqlite_error_code(),
        Some(rusqlite::ErrorCode::DatabaseBusy)
    );
    reader.execute_batch("ROLLBACK").unwrap();
    assert_eq!(
        c.pragma_query_value::<i64, _>(None, "user_version", |r| r.get(0))
            .unwrap(),
        16
    );
    assert_eq!(
        c.pragma_query_value::<i64, _>(None, "schema_version", |r| r.get(0))
            .unwrap(),
        cookie
    );
    assert_eq!(
        c.query_row(
            "SELECT count(*) FROM pragma_index_info('requests_account')",
            [],
            |r| r.get::<_, i64>(0)
        )
        .unwrap(),
        2
    );
    crate::schema::migrate(&mut c).unwrap();
    assert_account_index(&c);
}

#[test]
fn v17_account_range_and_same_millisecond_pagination() {
    let mut c = fixture(16);
    c.execute_batch(
        "INSERT INTO requests(id,account,transport,status,retries,switches,duration_ms)
        VALUES(1048577,1,1,200,0,0,1),(1048578,NULL,1,200,0,0,1),
              (1048579,1,1,200,0,0,1),(2097152,1,1,200,0,0,1)",
    )
    .unwrap();
    crate::schema::migrate(&mut c).unwrap();
    let mut q = RecentQuery {
        limit: 1,
        since_ms: Some(1),
        before_ms: Some(2),
        filters: Filters {
            account: Some("acct_01234567".into()),
            ..Default::default()
        },
        ..Default::default()
    };
    for id in [1048579, 1048577, 1048576] {
        let page = crate::records::recent(&c, &q, &PriceTable::default()).unwrap();
        assert_eq!(page.len(), 1);
        assert_eq!(page[0].request_id, format!("{id:016x}"));
        q.before_id = Some(id);
    }
    assert!(crate::records::recent(&c, &q, &PriceTable::default())
        .unwrap()
        .is_empty());
    let sum = crate::summary::summary(
        &c,
        &SummaryQuery {
            from_ms: 1,
            to_ms: 1,
            filters: q.filters,
            total_only: true,
            ..Default::default()
        },
        &PriceTable::default(),
    )
    .unwrap();
    assert_eq!(sum.total.count, 3);
}

#[test]
fn v18_preserves_pending_and_known_expiry_across_upgrades() {
    for version in [13, 15, 16, 17] {
        for preexisting in [false, true] {
            let mut c = fixture(version);
            c.execute_batch(
                "INSERT INTO reset_consume(account,credit_id,redeem_request_id,started_ms,status)
                VALUES('acct_01234567','card','same-id',123,'pending')",
            )
            .unwrap();
            if preexisting {
                c.execute_batch(
                    "ALTER TABLE reset_consume ADD COLUMN credit_expires_ms INTEGER;
                    UPDATE reset_consume SET credit_expires_ms=456",
                )
                .unwrap();
            }
            for _ in 0..2 {
                crate::schema::migrate(&mut c).unwrap();
                let row = c.query_row("SELECT account,credit_id,redeem_request_id,started_ms,status,credit_expires_ms
                    FROM reset_consume", [], |r| Ok((r.get::<_,String>(0)?,r.get::<_,String>(1)?,
                    r.get::<_,String>(2)?,r.get::<_,i64>(3)?,r.get::<_,String>(4)?,r.get::<_,Option<i64>>(5)?))).unwrap();
                assert_eq!(
                    row,
                    (
                        "acct_01234567".into(),
                        "card".into(),
                        "same-id".into(),
                        123,
                        "pending".into(),
                        preexisting.then_some(456)
                    )
                );
                assert_eq!(
                    c.pragma_query_value::<i64, _>(None, "user_version", |r| r.get(0))
                        .unwrap(),
                    crate::schema::SCHEMA_VERSION
                );
                assert_eq!(
                    c.query_row("SELECT count(*) FROM requests", [], |r| r.get::<_, i64>(0))
                        .unwrap(),
                    1
                );
            }
        }
    }
}

#[test]
fn v19_adds_credit_columns_and_keeps_rows() {
    // Build a v18 database: today's schema without the v19 additions.
    let mut c = fixture(16);
    crate::schema::migrate(&mut c).unwrap();
    c.execute_batch(
        "ALTER TABLE quota_obs DROP COLUMN cr_flags;
         ALTER TABLE quota_obs DROP COLUMN cr_balance;
         ALTER TABLE quota_cycle_estimates DROP COLUMN over_credits;
         ALTER TABLE quota_cycle_estimates DROP COLUMN over_api_usd;
         DROP TABLE quota_est_epochs;
         PRAGMA user_version=18;
         INSERT INTO quota_obs(id,account,p_pct,p_reset,p_win) VALUES(1048576,1,42,99,10080);
         INSERT INTO quota_cycle_estimates(account,window,started_ms,ended_ms,ended_obs_id,
           close_kind,credits_used,confidence) VALUES(1,'primary',1,2,1048576,'natural',77.5,'ok');",
    )
    .unwrap();
    for _ in 0..2 {
        crate::schema::migrate(&mut c).unwrap();
        let obs = c
            .query_row(
                "SELECT p_pct, cr_flags, cr_balance FROM quota_obs WHERE id=1048576",
                [],
                |r| {
                    Ok((
                        r.get::<_, f64>(0)?,
                        r.get::<_, Option<i64>>(1)?,
                        r.get::<_, Option<f64>>(2)?,
                    ))
                },
            )
            .unwrap();
        assert_eq!(obs, (42.0, None, None));
        let cycle = c
            .query_row(
                "SELECT credits_used, over_credits, over_api_usd FROM quota_cycle_estimates",
                [],
                |r| {
                    Ok((
                        r.get::<_, f64>(0)?,
                        r.get::<_, Option<f64>>(1)?,
                        r.get::<_, Option<f64>>(2)?,
                    ))
                },
            )
            .unwrap();
        assert_eq!(cycle, (77.5, None, None));
        assert_eq!(
            c.query_row("SELECT count(*) FROM quota_est_epochs", [], |r| r
                .get::<_, i64>(0))
                .unwrap(),
            0
        );
        assert_eq!(
            c.pragma_query_value::<i64, _>(None, "user_version", |r| r.get(0))
                .unwrap(),
            19
        );
    }
}
