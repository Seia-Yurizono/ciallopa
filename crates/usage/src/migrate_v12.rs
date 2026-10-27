//! One-shot schema v11 → v12 data rewrite: account labels become aliases, error_detail
//! bodies are content-trimmed, reset_consume accounts and refusal UAs are scrubbed.
use crate::detail::{keep_content_safe_str, redact_body, redact_text};
use crate::refusals;
use ciallopa_core::Aliaser;
use rusqlite::{params, Connection, OptionalExtension};
use std::collections::HashSet;
use std::sync::Arc;

const ALIAS_KEY: &str = "alias_key";
const DETAIL_BATCH: i64 = 1000;

/// Load or create the HMAC key inside an open connection (shared with Recorder).
pub(crate) fn load_or_create_aliaser(c: &Connection) -> rusqlite::Result<Arc<Aliaser>> {
    let existing: Option<String> = c
        .query_row("SELECT v FROM kv WHERE k = ?1", [ALIAS_KEY], |r| r.get(0))
        .optional()?;
    match existing {
        Some(s) => Aliaser::from_b64(&s).map_err(|e| {
            rusqlite::Error::ToSqlConversionFailure(Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                e,
            )))
        }),
        None => {
            let al = Aliaser::ephemeral();
            c.execute(
                "INSERT INTO kv(k, v) VALUES (?1, ?2)",
                params![ALIAS_KEY, al.key_b64()],
            )?;
            Ok(al)
        }
    }
}

/// Rewrite stored account identifiers and scrub historical error payloads.
/// Idempotent: already-aliased labels and already-trimmed bodies are left alone.
pub(crate) fn rewrite(c: &Connection) -> rusqlite::Result<()> {
    let al = load_or_create_aliaser(c)?;
    rename_account_labels(c, &al)?;
    rewrite_reset_consume(c, &al)?;
    rewrite_error_detail(c, &al)?;
    truncate_refusal_uas(c)?;
    Ok(())
}

fn account_label_ids(c: &Connection) -> rusqlite::Result<HashSet<i64>> {
    let mut ids = HashSet::new();
    let mut push = |sql: &str| -> rusqlite::Result<()> {
        let mut st = c.prepare(sql)?;
        let rows = st.query_map([], |r| r.get::<_, Option<i64>>(0))?;
        for row in rows {
            if let Some(id) = row? {
                ids.insert(id);
            }
        }
        Ok(())
    };
    push("SELECT DISTINCT account FROM requests WHERE account IS NOT NULL")?;
    push("SELECT DISTINCT first_account FROM requests WHERE first_account IS NOT NULL")?;
    push("SELECT DISTINCT account FROM quota_obs")?;
    push("SELECT DISTINCT account FROM refusals WHERE account != 0")?;
    Ok(ids)
}

fn rename_account_labels(c: &Connection, al: &Aliaser) -> rusqlite::Result<()> {
    let ids = account_label_ids(c)?;
    for id in ids {
        let name: String = match c
            .query_row("SELECT name FROM labels WHERE id = ?1", [id], |r| r.get(0))
            .optional()?
        {
            Some(n) => n,
            None => continue,
        };
        if Aliaser::looks_like_alias(&name) {
            let _ = al.alias(&name);
            continue;
        }
        let alias = al.alias(&name);
        if alias == name {
            continue;
        }
        let existing: Option<i64> = c
            .query_row("SELECT id FROM labels WHERE name = ?1", [&alias], |r| {
                r.get(0)
            })
            .optional()?;
        match existing {
            None => {
                c.execute(
                    "UPDATE labels SET name = ?1 WHERE id = ?2",
                    params![alias, id],
                )?;
            }
            Some(new_id) if new_id == id => {}
            Some(new_id) => {
                retarget_account_fk(c, id, new_id)?;
                c.execute("DELETE FROM labels WHERE id = ?1", [id])?;
            }
        }
    }
    Ok(())
}

fn retarget_account_fk(c: &Connection, old: i64, new: i64) -> rusqlite::Result<()> {
    c.execute(
        "UPDATE requests SET account = ?1 WHERE account = ?2",
        params![new, old],
    )?;
    c.execute(
        "UPDATE requests SET first_account = ?1 WHERE first_account = ?2",
        params![new, old],
    )?;
    c.execute(
        "UPDATE quota_obs SET account = ?1 WHERE account = ?2",
        params![new, old],
    )?;
    // Move non-conflicting refusal rows; drop the rest (historical counts, best-effort).
    c.execute(
        "UPDATE OR IGNORE refusals SET account = ?1 WHERE account = ?2",
        params![new, old],
    )?;
    c.execute("DELETE FROM refusals WHERE account = ?1", [old])?;
    Ok(())
}

fn rewrite_reset_consume(c: &Connection, al: &Aliaser) -> rusqlite::Result<()> {
    let mut st = c.prepare("SELECT id, account FROM reset_consume")?;
    let rows: Vec<(i64, String)> = st
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?
        .collect::<rusqlite::Result<_>>()?;
    drop(st);
    for (id, account) in rows {
        if Aliaser::looks_like_alias(&account) {
            continue;
        }
        let alias = al.alias(&account);
        if alias != account {
            c.execute(
                "UPDATE reset_consume SET account = ?1 WHERE id = ?2",
                params![alias, id],
            )?;
        }
    }
    Ok(())
}

/// One `error_detail` row: id, message, body, attempts.
type DetailRow = (i64, Option<String>, Option<String>, Option<String>);

fn rewrite_error_detail(c: &Connection, al: &Aliaser) -> rusqlite::Result<()> {
    let mut last_id: i64 = -1;
    loop {
        let mut st = c.prepare(
            "SELECT id, message, body, attempts FROM error_detail WHERE id > ?1 ORDER BY id LIMIT ?2",
        )?;
        let batch: Vec<DetailRow> = st
            .query_map(params![last_id, DETAIL_BATCH], |r| {
                Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?))
            })?
            .collect::<rusqlite::Result<_>>()?;
        drop(st);
        if batch.is_empty() {
            break;
        }
        for (id, message, body, attempts) in &batch {
            last_id = *id;
            let new_msg = message.as_deref().map(redact_text);
            let new_body = body
                .as_deref()
                .map(|b| redact_body(&keep_content_safe_str(b)));
            let new_attempts = attempts.as_deref().map(|a| alias_attempts_json(a, al));
            c.execute(
                "UPDATE error_detail SET message = ?1, body = ?2, attempts = ?3 WHERE id = ?4",
                params![new_msg, new_body, new_attempts, id],
            )?;
        }
        if (batch.len() as i64) < DETAIL_BATCH {
            break;
        }
    }
    Ok(())
}

fn alias_attempts_json(raw: &str, al: &Aliaser) -> String {
    let Ok(mut v) = serde_json::from_str::<serde_json::Value>(raw) else {
        return raw.to_string();
    };
    if let Some(arr) = v.as_array_mut() {
        for item in arr {
            if let Some(obj) = item.as_object_mut() {
                if let Some(acc) = obj.get("account").and_then(|x| x.as_str()) {
                    let a = al.alias(acc);
                    obj.insert("account".into(), serde_json::Value::String(a));
                }
                if let Some(snip) = obj.get("body_snippet").and_then(|x| x.as_str()) {
                    let cleaned = redact_body(&keep_content_safe_str(snip));
                    obj.insert("body_snippet".into(), serde_json::Value::String(cleaned));
                }
            }
        }
    }
    v.to_string()
}

fn truncate_refusal_uas(c: &Connection) -> rusqlite::Result<()> {
    let mut st = c.prepare(
        "SELECT DISTINCT l.id, l.name FROM labels l
         JOIN refusals r ON r.ua = l.id WHERE r.ua != 0",
    )?;
    let rows: Vec<(i64, String)> = st
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?
        .collect::<rusqlite::Result<_>>()?;
    drop(st);
    for (id, name) in rows {
        let clipped = refusals::clip_ua_for_migrate(&name);
        if clipped == name {
            continue;
        }
        let existing: Option<i64> = c
            .query_row("SELECT id FROM labels WHERE name = ?1", [&clipped], |r| {
                r.get(0)
            })
            .optional()?;
        match existing {
            None => {
                c.execute(
                    "UPDATE labels SET name = ?1 WHERE id = ?2",
                    params![clipped, id],
                )?;
            }
            Some(new_id) if new_id == id => {}
            Some(new_id) => {
                c.execute(
                    "UPDATE OR IGNORE refusals SET ua = ?1 WHERE ua = ?2",
                    params![new_id, id],
                )?;
                c.execute("DELETE FROM refusals WHERE ua = ?1", [id])?;
                // Drop the old label only if nothing else references it.
                let still: i64 = c.query_row(
                    "SELECT
                       (SELECT COUNT(*) FROM requests WHERE model=?1 OR key=?1 OR route=?1 OR instance=?1 OR account=?1 OR first_account=?1)
                     + (SELECT COUNT(*) FROM quota_obs WHERE account=?1)
                     + (SELECT COUNT(*) FROM refusals WHERE
                          model=?1 OR key=?1 OR route=?1 OR ua=?1 OR account=?1 OR instance=?1 OR lim=?1)",
                    [id],
                    |r| r.get(0),
                )?;
                if still == 0 {
                    c.execute("DELETE FROM labels WHERE id = ?1", [id])?;
                }
            }
        }
    }
    Ok(())
}
