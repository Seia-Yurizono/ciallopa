//! Manual reset-credit (重置卡) consume log. Rows are written synchronously (rare, admin-only)
//! on their own connection; the request writer thread is not involved.
use crate::{now_ms, store, Recorder, UsageError};
use rusqlite::{params, OptionalExtension};
use serde::Serialize;

/// Unknown-outcome attempts younger than this are retried with the same `redeem_request_id`.
pub const PENDING_REUSE_MS: i64 = 24 * 3_600_000;

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct ConsumeAttempt {
    pub id: i64,
    pub account: String,
    pub credit_id: Option<String>,
    pub redeem_request_id: String,
    pub key: Option<String>,
    pub instance: Option<String>,
    pub started_ms: i64,
    pub finished_ms: Option<i64>,
    /// `pending` (outcome unknown: retry reuses the id), `done`, `failed`.
    pub status: String,
    pub http_status: Option<i64>,
    pub code: Option<String>,
    pub windows_reset: Option<i64>,
    pub error: Option<String>,
}

/// Outcome written by [`Recorder::reset_finish`].
#[derive(Debug, Clone, Default)]
pub struct ConsumeOutcome {
    pub status: &'static str,
    pub http_status: Option<u16>,
    pub code: Option<String>,
    pub windows_reset: Option<i64>,
    pub error: Option<String>,
}

fn row(r: &rusqlite::Row<'_>) -> rusqlite::Result<ConsumeAttempt> {
    Ok(ConsumeAttempt {
        id: r.get(0)?,
        account: r.get(1)?,
        credit_id: r.get(2)?,
        redeem_request_id: r.get(3)?,
        key: r.get(4)?,
        instance: r.get(5)?,
        started_ms: r.get(6)?,
        finished_ms: r.get(7)?,
        status: r.get(8)?,
        http_status: r.get(9)?,
        code: r.get(10)?,
        windows_reset: r.get(11)?,
        error: r.get(12)?,
    })
}
const COLS: &str = "id, account, credit_id, redeem_request_id, key, instance, started_ms, \
                    finished_ms, status, http_status, code, windows_reset, error";

impl Recorder {
    /// Small persistent state (blocking; startup / rare writes only).
    pub fn kv_get(&self, k: &str) -> Result<Option<String>, UsageError> {
        let c = store::open_rw(&self.path)?;
        Ok(
            c.query_row("SELECT v FROM kv WHERE k=?1", params![k], |r| r.get(0))
                .optional()?,
        )
    }
    pub fn kv_set(&self, k: &str, v: &str) -> Result<(), UsageError> {
        let c = store::open_rw(&self.path)?;
        c.execute(
            "INSERT INTO kv(k, v) VALUES (?1, ?2) ON CONFLICT(k) DO UPDATE SET v=excluded.v",
            params![k, v],
        )?;
        Ok(())
    }

    /// Latest recent `pending` attempt of `account` (any credit): while one exists, every
    /// consume on the account must retry exactly that attempt (same id and credit).
    pub async fn reset_pending(&self, account: &str) -> Result<Option<ConsumeAttempt>, UsageError> {
        let (p, a) = (self.path.clone(), account.to_string());
        tokio::task::spawn_blocking(move || {
            let conn = store::open_rw(&p)?;
            Ok(conn
                .query_row(
                    &format!(
                        "SELECT {COLS} FROM reset_consume WHERE account=?1 \
                         AND status='pending' AND started_ms>=?2 ORDER BY id DESC LIMIT 1"
                    ),
                    params![a, now_ms() - PENDING_REUSE_MS],
                    row,
                )
                .optional()?)
        })
        .await
        .map_err(|_| UsageError::Gone)?
    }

    /// Insert a `pending` attempt; returns its row id.
    pub async fn reset_begin(
        &self,
        account: &str,
        credit_id: Option<&str>,
        redeem_request_id: &str,
        key: Option<&str>,
        instance: &str,
    ) -> Result<i64, UsageError> {
        let p = self.path.clone();
        let v = (
            account.to_string(),
            credit_id.map(str::to_string),
            redeem_request_id.to_string(),
            key.map(str::to_string),
            instance.to_string(),
        );
        tokio::task::spawn_blocking(move || {
            let conn = store::open_rw(&p)?;
            conn.execute(
                "INSERT INTO reset_consume(account, credit_id, redeem_request_id, key, instance, \
                 started_ms, status) VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'pending')",
                params![v.0, v.1, v.2, v.3, v.4, now_ms()],
            )?;
            Ok(conn.last_insert_rowid())
        })
        .await
        .map_err(|_| UsageError::Gone)?
    }

    pub async fn reset_finish(&self, id: i64, o: ConsumeOutcome) -> Result<(), UsageError> {
        let p = self.path.clone();
        tokio::task::spawn_blocking(move || {
            let conn = store::open_rw(&p)?;
            conn.execute(
                "UPDATE reset_consume SET finished_ms=?2, status=?3, http_status=?4, code=?5, \
                 windows_reset=?6, error=?7 WHERE id=?1",
                params![
                    id,
                    now_ms(),
                    o.status,
                    o.http_status.map(i64::from),
                    o.code,
                    o.windows_reset,
                    o.error
                ],
            )?;
            Ok(())
        })
        .await
        .map_err(|_| UsageError::Gone)?
    }

    /// Newest first.
    pub async fn reset_attempts(&self, limit: usize) -> Result<Vec<ConsumeAttempt>, UsageError> {
        let p = self.path.clone();
        tokio::task::spawn_blocking(move || {
            let conn = store::open_ro(&p)?;
            let mut st = conn.prepare(&format!(
                "SELECT {COLS} FROM reset_consume ORDER BY id DESC LIMIT ?1"
            ))?;
            let v = st
                .query_map(params![limit as i64], row)?
                .collect::<Result<Vec<_>, _>>()?;
            Ok(v)
        })
        .await
        .map_err(|_| UsageError::Gone)?
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn pending_roundtrip() {
        let d = tempfile::tempdir().unwrap();
        let cfg = ciallopa_core::config::UsageConfig {
            path: d.path().join("u.db"),
            ..Default::default()
        };
        let r = Recorder::open_as(&cfg, "i1").unwrap();
        assert!(r.reset_pending("a").await.unwrap().is_none());
        let id = r
            .reset_begin("a", None, "rid-1", Some("k"), "i1")
            .await
            .unwrap();
        let p = r.reset_pending("a").await.unwrap().unwrap();
        assert_eq!(p.redeem_request_id, "rid-1");
        // Account-wide: a card-level attempt must see the account-level pending one.
        assert!(r.reset_pending("a").await.unwrap().is_some());
        assert!(r.reset_pending("b").await.unwrap().is_none());
        r.reset_finish(
            id,
            ConsumeOutcome {
                status: "done",
                http_status: Some(200),
                code: Some("reset".into()),
                windows_reset: Some(2),
                error: None,
            },
        )
        .await
        .unwrap();
        assert!(r.reset_pending("a").await.unwrap().is_none());
        let all = r.reset_attempts(10).await.unwrap();
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].code.as_deref(), Some("reset"));
        assert_eq!(all[0].windows_reset, Some(2));
        r.shutdown();
    }
}
