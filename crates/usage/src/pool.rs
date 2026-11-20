//! Reused SQLite read connections; permits bound both blocking work and page caches.
use crate::{schema, UsageError};
use rusqlite::{Connection, ErrorCode, InterruptHandle};
use std::{
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
};
use tokio::sync::Semaphore;

const PROGRESS_OPS: i32 = 4096;

#[derive(Default)]
struct CancelOnDrop {
    cancelled: Arc<AtomicBool>,
    handle: Arc<Mutex<Option<InterruptHandle>>>,
}

impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        let handle = self.handle.lock().expect("read interrupt lock");
        self.cancelled.store(true, Ordering::Relaxed);
        if let Some(handle) = handle.as_ref() {
            handle.interrupt();
        }
    }
}

fn read_sql_error(error: rusqlite::Error, cancelled: bool) -> UsageError {
    if cancelled && error.sqlite_error_code() == Some(ErrorCode::OperationInterrupted) {
        UsageError::Cancelled
    } else {
        UsageError::Sql(error)
    }
}

pub(crate) struct ReadPool {
    path: PathBuf,
    conns: Mutex<Vec<Connection>>,
    permits: Arc<Semaphore>,
}
impl ReadPool {
    pub fn new(path: PathBuf) -> Arc<Self> {
        Arc::new(Self {
            path,
            conns: Mutex::new(Vec::new()),
            permits: Arc::new(Semaphore::new(4)),
        })
    }
    pub async fn read<T: Send + 'static>(
        self: &Arc<Self>,
        f: impl FnOnce(&Connection) -> Result<T, UsageError> + Send + 'static,
    ) -> Result<T, UsageError> {
        let permit = self
            .permits
            .clone()
            .acquire_owned()
            .await
            .expect("read pool stays open");
        let pool = self.clone();
        let guard = CancelOnDrop::default();
        let (cancelled, handle) = (guard.cancelled.clone(), guard.handle.clone());
        tokio::task::spawn_blocking(move || {
            // The permit stays with the blocking work even if its async caller is cancelled.
            let _permit = permit;
            let idle = pool.conns.lock().unwrap_or_else(|e| e.into_inner()).pop();
            let mut conn = match idle {
                Some(conn) => conn,
                None => schema::open_read(&pool.path)?,
            };
            {
                let mut handle = handle.lock().expect("read interrupt lock");
                if cancelled.load(Ordering::Relaxed) {
                    return Err(UsageError::Cancelled);
                }
                *handle = Some(conn.get_interrupt_handle());
            }
            // interrupt() only affects active statements. The hook also covers gaps between them.
            let flag = cancelled.clone();
            conn.progress_handler(PROGRESS_OPS, Some(move || flag.load(Ordering::Relaxed)))?;
            let tx = conn
                .transaction()
                .map_err(|e| read_sql_error(e, cancelled.load(Ordering::Relaxed)))?;
            let result = f(&tx);

            // Retire under the same lock as interrupt() before this connection can be reused.
            let was_cancelled = {
                let mut handle = handle.lock().expect("read interrupt lock");
                handle.take();
                cancelled.load(Ordering::Relaxed)
            };
            let result = match result {
                Ok(_) if was_cancelled => Err(UsageError::Cancelled),
                Err(UsageError::Sql(e)) => Err(read_sql_error(e, was_cancelled)),
                other => other,
            };
            let finish = if result.is_ok() {
                tx.commit()
            } else {
                tx.rollback()
            };
            let clean = finish.and_then(|()| conn.progress_handler(0, None::<fn() -> bool>));
            if clean.is_ok() && conn.is_autocommit() {
                pool.conns
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .push(conn);
            }
            let value = result?;
            clean.map_err(|e| read_sql_error(e, cancelled.load(Ordering::Relaxed)))?;
            Ok(value)
        })
        .await?
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use futures::poll;
    use std::sync::atomic::AtomicUsize;
    use std::time::Duration;
    use tokio::{sync::oneshot, time::timeout};

    #[tokio::test]
    async fn cancelled_scan_yields_same_connection_to_next_reader() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("u.db");
        let _writer = schema::open_and_migrate(&path).unwrap();
        let pool = ReadPool::new(path);
        let _reserved = pool.permits.acquire_many(3).await.unwrap();
        pool.read(|c| Ok(c.execute_batch("CREATE TEMP TABLE marker AS SELECT 42 AS value")?))
            .await
            .unwrap();

        for active_statement in [true, false] {
            let (started, ready) = oneshot::channel();
            let (resume, gate) = std::sync::mpsc::channel();
            let (finished, outcome) = oneshot::channel();
            let mut a = Box::pin(pool.read(move |c| {
                let mut stmt = c.prepare(
                    "WITH RECURSIVE ns(n) AS (
                    VALUES(1) UNION ALL SELECT n+1 FROM ns WHERE n < 100000000
                ) SELECT 0 UNION ALL SELECT sum(n) FROM ns",
                )?;
                let mut rows = stmt.query([])?;
                if active_statement {
                    rows.next()?;
                }
                started.send(c.get_interrupt_handle()).ok().unwrap();
                if !active_statement {
                    gate.recv().unwrap(); // interrupt() while SQLite has no active statement
                }
                let scan = rows.mapped(|_| Ok(())).collect::<rusqlite::Result<()>>();
                finished
                    .send(
                        scan.as_ref()
                            .err()
                            .and_then(rusqlite::Error::sqlite_error_code),
                    )
                    .unwrap();
                Ok(scan?)
            }));
            assert!(poll!(&mut a).is_pending());
            let rescue = ready.await.unwrap();
            let mut b = Box::pin(pool.read(|c| {
                Ok(c.query_row("SELECT value FROM marker", [], |r| r.get::<_, i64>(0))?)
            }));
            assert!(poll!(&mut b).is_pending());
            drop(a);
            if !active_statement {
                resume.send(()).unwrap();
            }
            let next = timeout(Duration::from_secs(3), b).await;
            if next.is_err() {
                rescue.interrupt(); // stop the long fixture even if cancellation regresses
            }
            assert_eq!(
                next.expect("cancelled scan held the read slot").unwrap(),
                42
            );
            assert_eq!(
                outcome.await.unwrap(),
                Some(ErrorCode::OperationInterrupted)
            );
        }
    }

    #[tokio::test]
    async fn dropping_completed_read_while_next_statement_is_active() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("u.db");
        let _writer = schema::open_and_migrate(&path).unwrap();
        let pool = ReadPool::new(path);
        let _reserved = pool.permits.acquire_many(3).await.unwrap();

        let (finish_a, gate_a) = std::sync::mpsc::channel();
        let mut a = Box::pin(pool.read(move |c| {
            gate_a.recv().unwrap();
            Ok(c.execute_batch("CREATE TEMP TABLE marker(value)")?)
        }));
        assert!(poll!(&mut a).is_pending());
        finish_a.send(()).unwrap();
        let (started, ready) = oneshot::channel();
        let (resume, gate) = std::sync::mpsc::channel();
        let next_pool = pool.clone();
        let b = tokio::spawn(async move {
            next_pool
                .read(move |c| {
                    Ok(c.query_row("SELECT count(*) FROM marker", [], |_| {
                        started.send(()).unwrap();
                        gate.recv().unwrap();
                        Ok(c.is_interrupted())
                    })?)
                })
                .await
        });
        ready.await.unwrap();
        drop(a); // Its worker returned the connection; its async guard is still armed.
        resume.send(()).unwrap();
        assert!(!b.await.unwrap().unwrap());
    }

    #[tokio::test]
    async fn overlapping_quota_callers_run_one_query_before_refreshing() {
        use rusqlite::hooks::{AuthAction, AuthContext, Authorization, TransactionOperation};

        let dir = tempfile::tempdir().unwrap();
        let rec = crate::Recorder::open(&crate::tests::cfg(dir.path())).unwrap();
        let pool = &rec.reads;
        let _reserved = pool.permits.acquire_many(3).await.unwrap();
        let last_slot = pool.permits.acquire().await.unwrap();
        let transactions = Arc::new(AtomicUsize::new(0));
        let count = transactions.clone();
        let mut conn = schema::open_read(rec.path()).unwrap();
        conn.authorizer(Some(move |ctx: AuthContext<'_>| {
            if matches!(
                ctx.action,
                AuthAction::Transaction {
                    operation: TransactionOperation::Begin
                }
            ) {
                count.fetch_add(1, Ordering::Relaxed);
            }
            Authorization::Allow
        }))
        .unwrap();
        pool.conns.lock().unwrap().push(conn);

        let mut callers: Vec<_> = (0..8).map(|_| Box::pin(rec.quota())).collect();
        for caller in &mut callers {
            assert!(poll!(caller).is_pending());
        }
        drop(callers.remove(0));
        drop(last_slot);
        assert!(callers.pop().unwrap().await.unwrap().is_empty());
        assert_eq!(transactions.load(Ordering::Relaxed), 1);
        // The other callers still hold the old result. A new request takes a fresh snapshot.
        assert!(rec.quota().await.unwrap().is_empty());
        for caller in callers {
            assert!(caller.await.unwrap().is_empty());
        }
        assert_eq!(transactions.load(Ordering::Relaxed), 2);

        let last_slot = pool.permits.acquire().await.unwrap();
        let mut abandoned = Box::pin(rec.quota());
        assert!(poll!(&mut abandoned).is_pending());
        drop(abandoned);
        drop(last_slot);
        assert!(
            pool.permits.try_acquire().is_ok(),
            "last waiter left its read in the queue"
        );
    }

    #[tokio::test]
    async fn reuses_connections_rolls_back_errors_and_reports_panics() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("u.db");
        let _writer = schema::open_and_migrate(&path).unwrap();
        let pool = ReadPool::new(path);
        pool.read(|c| {
            c.execute_batch("CREATE TEMP TABLE reusable(value)")?;
            Ok(())
        })
        .await
        .unwrap();
        assert!(pool
            .read::<()>(|c| {
                c.execute("INSERT INTO reusable VALUES (1)", [])?;
                Err(UsageError::Gone)
            })
            .await
            .is_err());
        let count: i64 = pool
            .read(|c| Ok(c.query_row("SELECT count(*) FROM reusable", [], |r| r.get(0))?))
            .await
            .unwrap();
        assert_eq!(count, 0);
        assert!(matches!(
            pool.read::<()>(|_| panic!("test read panic")).await,
            Err(UsageError::Join(_))
        ));
        assert_eq!(pool.permits.available_permits(), 4);
    }

    #[tokio::test]
    async fn bounds_work_and_keeps_permits_until_cancelled_reads_finish() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("u.db");
        let _writer = schema::open_and_migrate(&path).unwrap();
        let pool = ReadPool::new(path);
        let gate = Arc::new((Mutex::new(false), std::sync::Condvar::new()));
        let (started, mut notifications) = tokio::sync::mpsc::unbounded_channel();
        let counts = Arc::new((AtomicUsize::new(0), AtomicUsize::new(0)));
        let mut tasks = Vec::new();
        for _ in 0..8 {
            let (gate, started, counts) = (gate.clone(), started.clone(), counts.clone());
            let mut task = Box::pin(pool.read(move |_| {
                let (active, peak) = &*counts;
                peak.fetch_max(
                    active.fetch_add(1, Ordering::Relaxed) + 1,
                    Ordering::Relaxed,
                );
                started.send(()).unwrap();
                let (lock, ready) = &*gate;
                let _guard = ready
                    .wait_while(lock.lock().unwrap(), |open| !*open)
                    .unwrap();
                active.fetch_sub(1, Ordering::Relaxed);
                Ok(())
            }));
            assert!(poll!(&mut task).is_pending());
            tasks.push(task);
        }
        for _ in 0..4 {
            timeout(Duration::from_secs(5), notifications.recv())
                .await
                .unwrap()
                .unwrap();
        }
        assert_eq!(pool.permits.available_permits(), 0);
        assert!(notifications.try_recv().is_err());
        drop(tasks.drain(..4));
        // Dropping the four running callers cannot free their blocking workers' permits.
        assert_eq!(pool.permits.available_permits(), 0);
        *gate.0.lock().unwrap() = true;
        gate.1.notify_all();
        timeout(Duration::from_secs(5), futures::future::try_join_all(tasks))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(counts.1.load(Ordering::Relaxed), 4);
        assert_eq!(pool.permits.available_permits(), 4);
    }
}
