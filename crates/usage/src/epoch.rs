//! Manual estimate resets (schema v19). A reset adds one `quota_est_epochs` row per window;
//! estimates then read only snapshots and requests at or after its `from_id`. Undo marks the
//! rows `undone_ms` within [`UNDO_WINDOW_MS`]. Nothing is deleted.
use crate::classify::WindowKind;
use crate::write::WriterState;
use crate::{UsageError, SEQ_BITS};
use rusqlite::{params, Connection, OptionalExtension, Transaction};
use serde::Serialize;
use std::sync::mpsc::SyncSender;

/// How long a reset can be undone.
pub const UNDO_WINDOW_MS: i64 = 30 * 86_400_000;

pub(crate) enum Op {
    Reset {
        account: String,
        windows: Vec<WindowKind>,
        from_id: i64,
        by_key: Option<String>,
    },
    Undo {
        account: String,
        grp: i64,
    },
}

pub(crate) struct Command {
    pub op: Op,
    pub reply: SyncSender<Result<Reply, UsageError>>,
}

pub(crate) enum Reply {
    Reset(EpochReset),
    Undone(u64),
}
impl Reply {
    pub fn reset(self) -> Result<EpochReset, UsageError> {
        match self {
            Self::Reset(r) => Ok(r),
            Self::Undone(_) => Err(UsageError::Gone),
        }
    }
    pub fn undone(self) -> Result<u64, UsageError> {
        match self {
            Self::Undone(n) => Ok(n),
            Self::Reset(_) => Err(UsageError::Gone),
        }
    }
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct EpochReset {
    /// Id of this reset (used by undo).
    pub epoch_id: i64,
    pub from_ms: i64,
    pub windows: Vec<&'static str>,
    /// Closed cycles of those windows that no longer count (kept, shown as archived).
    pub archived_cycles: i64,
    pub undo_until_ms: i64,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct EpochRow {
    pub id: i64,
    pub window: String,
    pub from_ms: i64,
    pub created_ms: i64,
    pub undone_ms: Option<i64>,
    pub undo_until_ms: i64,
    /// The newest reset of this window that is not undone.
    pub active: bool,
}

/// `from_id` of the newest live reset of this window.
pub(crate) fn since(
    c: &Connection,
    account: i64,
    which: WindowKind,
) -> rusqlite::Result<Option<i64>> {
    c.prepare_cached(
        "SELECT max(from_id) FROM quota_est_epochs \
         WHERE account=?1 AND window=?2 AND undone_ms IS NULL",
    )?
    .query_row(params![account, which.as_str()], |r| r.get(0))
}

/// Resets of an account, newest first: active ones and anything still undoable.
pub(crate) fn rows(c: &Connection, account: i64, now_ms: i64) -> rusqlite::Result<Vec<EpochRow>> {
    let mut st = c.prepare_cached(
        "SELECT grp, window, from_id, created_ms, undone_ms FROM quota_est_epochs e \
         WHERE account=?1 AND (created_ms >= ?2 OR (undone_ms IS NULL AND from_id = \
           (SELECT max(from_id) FROM quota_est_epochs x WHERE x.account=e.account \
            AND x.window=e.window AND x.undone_ms IS NULL))) \
         ORDER BY from_id DESC, window",
    )?;
    let rows = st
        .query_map(params![account, now_ms - UNDO_WINDOW_MS], |r| {
            Ok((
                r.get::<_, i64>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, i64>(2)?,
                r.get::<_, i64>(3)?,
                r.get::<_, Option<i64>>(4)?,
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let mut seen = std::collections::HashSet::new();
    Ok(rows
        .into_iter()
        .map(|(id, window, from_id, created_ms, undone_ms)| {
            let active = undone_ms.is_none() && seen.insert(window.clone());
            EpochRow {
                id,
                window,
                from_ms: from_id >> SEQ_BITS,
                created_ms,
                undone_ms,
                undo_until_ms: created_ms + UNDO_WINDOW_MS,
                active,
            }
        })
        .collect())
}

fn account_id(tx: &Transaction<'_>, name: &str) -> rusqlite::Result<Option<i64>> {
    tx.query_row("SELECT id FROM labels WHERE name=?1", [name], |r| r.get(0))
        .optional()
}

pub(crate) fn apply(
    tx: &Transaction<'_>,
    ws: &mut WriterState,
    op: &Op,
) -> Result<Reply, UsageError> {
    let now_ms = crate::now_ms();
    match op {
        Op::Reset {
            account,
            windows,
            from_id,
            by_key,
        } => {
            let a = account_id(tx, account)?.ok_or(UsageError::NotFound("account"))?;
            let mut grp = None::<i64>;
            for w in windows {
                tx.execute(
                    "INSERT INTO quota_est_epochs(grp,account,window,from_id,created_ms,by_key) \
                     VALUES (coalesce(?1, 0),?2,?3,?4,?5,?6)",
                    params![grp, a, w.as_str(), from_id, now_ms, by_key],
                )?;
                let id = tx.last_insert_rowid();
                let g = *grp.get_or_insert(id);
                tx.execute(
                    "UPDATE quota_est_epochs SET grp=?1 WHERE id=?2",
                    params![g, id],
                )?;
            }
            // Re-observe the latest snapshot at the reset so the new estimate has an origin.
            tx.execute(
                "INSERT OR IGNORE INTO quota_obs(id,account,instance,plan,p_pct,p_reset,p_win,\
                 s_pct,s_reset,s_win,cards,cr_flags,cr_balance) \
                 SELECT ?1,account,instance,plan,p_pct,p_reset,p_win,s_pct,s_reset,s_win,cards,\
                 cr_flags,cr_balance FROM quota_obs WHERE account=?2 AND id<?1 \
                 ORDER BY id DESC LIMIT 1",
                params![from_id, a],
            )?;
            ws.forget_quota();
            let from_ms = from_id >> SEQ_BITS;
            let mut archived = 0;
            for w in windows {
                archived += tx.query_row(
                    "SELECT count(*) FROM quota_cycle_estimates \
                     WHERE account=?1 AND window=?2 AND ended_ms<?3",
                    params![a, w.as_str(), from_ms],
                    |r| r.get::<_, i64>(0),
                )?;
            }
            Ok(Reply::Reset(EpochReset {
                epoch_id: grp.unwrap_or(0),
                from_ms,
                windows: windows.iter().map(|w| w.as_str()).collect(),
                archived_cycles: archived,
                undo_until_ms: now_ms + UNDO_WINDOW_MS,
            }))
        }
        Op::Undo { account, grp } => {
            let a = account_id(tx, account)?.ok_or(UsageError::NotFound("account"))?;
            let n = tx.execute(
                "UPDATE quota_est_epochs SET undone_ms=?1 \
                 WHERE account=?2 AND grp=?3 AND undone_ms IS NULL AND created_ms>=?4",
                params![now_ms, a, grp, now_ms - UNDO_WINDOW_MS],
            )?;
            if n == 0 {
                return Err(UsageError::NotFound("reset"));
            }
            Ok(Reply::Undone(n as u64))
        }
    }
}

/// Undone resets past the undo window carry no meaning any more.
pub(crate) fn purge(c: &Connection, now_ms: i64) -> rusqlite::Result<()> {
    c.execute(
        "DELETE FROM quota_est_epochs WHERE undone_ms IS NOT NULL AND created_ms < ?1",
        [now_ms - UNDO_WINDOW_MS],
    )?;
    Ok(())
}
