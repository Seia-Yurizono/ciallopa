//! Reused SQLite read connections; permits bound both blocking work and page caches.
use crate::{schema, UsageError};
use rusqlite::Connection;
use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
};
use tokio::sync::Semaphore;

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
        tokio::task::spawn_blocking(move || {
            // The permit stays with the blocking work even if its async caller is cancelled.
            let _permit = permit;
            let mut conn = pool
                .conns
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .pop()
                .map(Ok)
                .unwrap_or_else(|| schema::open_read(&pool.path))?;
            let tx = conn.transaction()?;
            let result = f(&tx);
            match result {
                Ok(value) => {
                    tx.commit()?;
                    pool.conns
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .push(conn);
                    Ok(value)
                }
                Err(error) => {
                    // Discard connections whose rollback failed.
                    if tx.rollback().is_ok() {
                        pool.conns
                            .lock()
                            .unwrap_or_else(|e| e.into_inner())
                            .push(conn);
                    }
                    Err(error)
                }
            }
        })
        .await?
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

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
        let mut tasks = Vec::new();
        for _ in 0..8 {
            let (pool, gate, started) = (pool.clone(), gate.clone(), started.clone());
            tasks.push(tokio::spawn(async move {
                pool.read(move |_| {
                    started.send(()).unwrap();
                    let (lock, ready) = &*gate;
                    let _guard = ready
                        .wait_while(lock.lock().unwrap(), |open| !*open)
                        .unwrap();
                    Ok(())
                })
                .await
            }));
        }
        for _ in 0..4 {
            tokio::time::timeout(std::time::Duration::from_secs(5), notifications.recv())
                .await
                .unwrap()
                .unwrap();
        }
        assert_eq!(pool.permits.available_permits(), 0);
        assert!(notifications.try_recv().is_err());
        for task in &tasks {
            task.abort();
        }
        // Aborting the async caller cannot free the permits held by blocking closures.
        assert_eq!(pool.permits.available_permits(), 0);
        *gate.0.lock().unwrap() = true;
        gate.1.notify_all();
        let _permits = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            pool.permits.acquire_many(4),
        )
        .await
        .unwrap()
        .unwrap();
    }
}
