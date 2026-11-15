//! Admin reset attempts use their own connection, independent of the request writer.
use crate::{now_ms, schema, Recorder, UsageError};
use rusqlite::{params, OptionalExtension};
use serde::Serialize;

/// Unknown-outcome attempts younger than this are retried with the same `redeem_request_id`.
pub const PENDING_REUSE_MS: i64 = 24 * 3_600_000;

/// Stored strings and serialized JSON remain compatible with existing reset logs.
#[derive(Debug, Clone, Copy, Default, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ConsumeStatus {
    #[default]
    Pending,
    Done,
    Failed,
    Superseded,
    Expired,
}
impl ConsumeStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Done => "done",
            Self::Failed => "failed",
            Self::Superseded => "superseded",
            Self::Expired => "expired",
        }
    }
}
impl TryFrom<&str> for ConsumeStatus {
    type Error = rusqlite::types::FromSqlError;
    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "pending" => Ok(Self::Pending),
            "done" => Ok(Self::Done),
            "failed" => Ok(Self::Failed),
            "superseded" => Ok(Self::Superseded),
            "expired" => Ok(Self::Expired),
            _ => Err(rusqlite::types::FromSqlError::InvalidType),
        }
    }
}
impl rusqlite::types::FromSql for ConsumeStatus {
    fn column_result(value: rusqlite::types::ValueRef<'_>) -> rusqlite::types::FromSqlResult<Self> {
        Self::try_from(value.as_str()?)
    }
}
impl rusqlite::ToSql for ConsumeStatus {
    fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
        Ok(self.as_str().into())
    }
}

/// Card selected for a consume attempt, with expiry known before sending.
#[derive(Clone, Copy)]
pub struct CreditRef<'a> {
    pub id: &'a str,
    pub expires_ms: Option<i64>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct ConsumeAttempt {
    pub id: i64,
    pub account: String,
    pub credit_id: Option<String>,
    pub credit_expires_ms: Option<i64>,
    pub redeem_request_id: String,
    pub key: Option<String>,
    pub instance: Option<String>,
    pub started_ms: i64,
    pub finished_ms: Option<i64>,
    /// Expired automatic attempts retain an unknown upstream outcome.
    pub status: ConsumeStatus,
    pub http_status: Option<i64>,
    pub code: Option<String>,
    pub windows_reset: Option<i64>,
    pub error: Option<String>,
}

/// Outcome written by [`Recorder::reset_finish`].
#[derive(Debug, Clone, Default)]
pub struct ConsumeOutcome {
    pub status: ConsumeStatus,
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
        credit_expires_ms: r.get(13)?,
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
                    finished_ms, status, http_status, code, windows_reset, error, credit_expires_ms";

impl Recorder {
    /// Startup/shutdown reuse one non-migrating KV connection.
    pub fn kv_get(&self, k: &str) -> Result<Option<String>, UsageError> {
        self.with_kv(|c| {
            Ok(c.prepare_cached("SELECT v FROM kv WHERE k=?1")?
                .query_row([k], |r| r.get(0))
                .optional()?)
        })
    }
    pub fn kv_set(&self, k: &str, v: &str) -> Result<(), UsageError> {
        self.with_kv(|c| {
            c.prepare_cached(
                "INSERT INTO kv(k, v) VALUES (?1, ?2) ON CONFLICT(k) DO UPDATE SET v=excluded.v",
            )?
            .execute(params![k, v])?;
            Ok(())
        })
    }
    fn with_kv<T>(
        &self,
        f: impl FnOnce(&rusqlite::Connection) -> Result<T, UsageError>,
    ) -> Result<T, UsageError> {
        let mut conn = self.kv.lock().unwrap_or_else(|e| e.into_inner());
        if conn.is_none() {
            *conn = Some(schema::open_write(&self.path)?);
        }
        f(conn.as_ref().expect("kv connection initialized"))
    }
    /// Latest recent `pending` attempt of `account` (any credit): while one exists, every
    /// consume on the account must retry exactly that attempt (same id and credit).
    pub async fn reset_pending(&self, account: &str) -> Result<Option<ConsumeAttempt>, UsageError> {
        let account = self.alias_account(account);
        self.read(move |conn| Ok(conn.prepare_cached(&format!(
            "SELECT {COLS} FROM reset_consume WHERE account=?1 AND status='pending' AND started_ms>=?2 ORDER BY id DESC LIMIT 1"
        ))?.query_row(params![account, now_ms() - PENDING_REUSE_MS], row).optional()?)).await
    }

    /// Insert a `pending` attempt on the caller's clock; returns its row id.
    pub async fn reset_begin(
        &self,
        account: &str,
        credit: Option<CreditRef<'_>>,
        redeem_request_id: &str,
        key: Option<&str>,
        instance: &str,
        started_ms: i64,
    ) -> Result<i64, UsageError> {
        let p = self.path.clone();
        let v = (
            self.alias_account(account),
            credit.map(|c| c.id.to_string()),
            redeem_request_id.to_string(),
            key.map(str::to_string),
            instance.to_string(),
            credit.and_then(|c| c.expires_ms),
        );
        tokio::task::spawn_blocking(move || {
            let conn = schema::open_write(&p)?;
            conn.execute(
                "INSERT INTO reset_consume(account, credit_id, redeem_request_id, key, instance, \
                 started_ms, status, credit_expires_ms) VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'pending', ?7)",
                params![v.0, v.1, v.2, v.3, v.4, started_ms, v.5],
            )?;
            Ok(conn.last_insert_rowid())
        })
        .await?
    }

    pub async fn reset_finish(&self, id: i64, o: ConsumeOutcome) -> Result<(), UsageError> {
        let p = self.path.clone();
        tokio::task::spawn_blocking(move || {
            let conn = schema::open_write(&p)?;
            conn.execute(
                "UPDATE reset_consume SET finished_ms=?2, status=?3, http_status=?4, code=?5, \
                 windows_reset=?6, error=?7 WHERE id=?1",
                params![
                    id,
                    now_ms(),
                    o.status,
                    o.http_status.map(i64::from),
                    o.code.map(|c| c.to_ascii_lowercase()),
                    o.windows_reset,
                    o.error
                ],
            )?;
            Ok(())
        })
        .await?
    }

    /// Account-scoped recovery history, newest first. Automatic use starts at most 24 h
    /// before expiry, so older rows cannot affect an unexpired card's retry budget.
    /// This is independent of the global page's display limit and keeps stored aliases.
    pub async fn reset_recent_attempts(
        &self,
        account: &str,
    ) -> Result<Vec<ConsumeAttempt>, UsageError> {
        let a = self.alias_account(account);
        self.read(move |conn| {
            let mut st = conn.prepare_cached(&format!(
                "SELECT {COLS} FROM reset_consume WHERE account=?1 AND started_ms>=?2 ORDER BY id DESC"
            ))?;
            let rows = st.query_map(params![a, now_ms() - PENDING_REUSE_MS], row)?
                .collect::<Result<Vec<_>, _>>()?;
            Ok(rows)
        }).await
    }

    /// Newest first (display only; use `reset_recent_attempts` for recovery).
    pub async fn reset_attempts(&self, limit: usize) -> Result<Vec<ConsumeAttempt>, UsageError> {
        self.read(move |conn| {
            let mut st = conn.prepare_cached(&format!(
                "SELECT {COLS} FROM reset_consume ORDER BY id DESC LIMIT ?1"
            ))?;
            let rows = st
                .query_map([limit as i64], row)?
                .collect::<Result<Vec<_>, _>>()?;
            Ok(rows)
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn known_expiry_survives_reopening_the_recorder() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = ciallopa_core::config::UsageConfig {
            path: dir.path().join("u.db"),
            ..Default::default()
        };
        let expires = now_ms() + 600_000;
        let rec = Recorder::open_as(&cfg, "i1").unwrap();
        rec.load_or_create_aliaser().unwrap();
        rec.reset_begin(
            "a",
            Some(CreditRef {
                id: "card",
                expires_ms: Some(expires),
            }),
            "rid",
            Some("(auto)"),
            "i1",
            now_ms(),
        )
        .await
        .unwrap();
        rec.shutdown();
        drop(rec);
        let rec = Recorder::open_as(&cfg, "i1").unwrap();
        rec.load_or_create_aliaser().unwrap();
        let pending = rec.reset_pending("a").await.unwrap().unwrap();
        assert_eq!(pending.redeem_request_id, "rid");
        assert_eq!(pending.credit_expires_ms, Some(expires));
        assert_eq!(rec.reset_recent_attempts("a").await.unwrap()[0], pending);
        rec.shutdown();
    }

    #[tokio::test]
    async fn pending_roundtrip() {
        let d = tempfile::tempdir().unwrap();
        let cfg = ciallopa_core::config::UsageConfig {
            path: d.path().join("u.db"),
            ..Default::default()
        };
        let r = Recorder::open_as(&cfg, "i1").unwrap();
        r.load_or_create_aliaser().unwrap();
        assert!(r.reset_pending("a").await.unwrap().is_none());
        let id = r
            .reset_begin("a", None, "rid-1", Some("k"), "i1", now_ms())
            .await
            .unwrap();
        let p = r.reset_pending("a").await.unwrap().unwrap();
        assert_eq!(p.redeem_request_id, "rid-1");
        assert_eq!(p.account, r.alias_account("a"));
        // Account-wide: a card-level attempt must see the account-level pending one.
        assert!(r.reset_pending("a").await.unwrap().is_some());
        assert!(r.reset_pending("b").await.unwrap().is_none());
        r.reset_finish(
            id,
            ConsumeOutcome {
                status: ConsumeStatus::Done,
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
        assert_eq!(r.reset_recent_attempts("a").await.unwrap(), all);
        assert!(r.reset_recent_attempts("b").await.unwrap().is_empty());
        r.shutdown();
    }
}
