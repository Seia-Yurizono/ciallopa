use async_trait::async_trait;
use ciallopa_core::*;
use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};

/// Fixed credentials and a precomputed snapshot, with explicit mutations for admin tests.
pub struct FixedStore {
    accounts: RwLock<Arc<[Account]>>,
    generation: AtomicU64,
    rotating_tokens: bool,
    pub refreshed: AtomicUsize,
    pub relogin: Mutex<HashSet<AccountId>>,
}
impl FixedStore {
    pub fn new(ids: &[&str]) -> Self {
        Self::with_accounts(
            ids.iter()
                .map(|id| Account {
                    id: (*id).into(),
                    email: None,
                    chatgpt_account_id: Some(format!("acct-{id}")),
                    plan_type: None,
                    priority: 0,
                    weight: 1,
                    disabled: false,
                })
                .collect(),
        )
    }
    pub fn with_accounts(mut accounts: Vec<Account>) -> Self {
        accounts.sort_by(|a, b| a.id.cmp(&b.id));
        Self {
            accounts: RwLock::new(accounts.into()),
            generation: AtomicU64::new(0),
            rotating_tokens: false,
            refreshed: AtomicUsize::new(0),
            relogin: Mutex::default(),
        }
    }
    pub fn rotating(ids: &[&str]) -> Self {
        Self {
            rotating_tokens: true,
            ..Self::new(ids)
        }
    }
    pub fn update(&self, change: impl FnOnce(&mut Vec<Account>)) {
        let mut snapshot = self.accounts.write().unwrap();
        let mut accounts = snapshot.to_vec();
        change(&mut accounts);
        accounts.sort_by(|a, b| a.id.cmp(&b.id));
        *snapshot = accounts.into();
        self.generation.fetch_add(1, Ordering::Release);
    }
}
#[async_trait]
impl AccountStore for FixedStore {
    fn accounts(&self) -> Arc<[Account]> {
        self.accounts.read().unwrap().clone()
    }
    fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }
    async fn credentials(&self, id: &AccountId) -> Result<Credentials, AuthError> {
        let access_token = if self.rotating_tokens {
            let generation = if self.refreshed.load(Ordering::SeqCst) > 0 {
                2
            } else {
                1
            };
            format!("tok-{id}-{generation}")
        } else {
            format!("tok-{id}")
        };
        Ok(Credentials {
            account: id.clone(),
            access_token,
            chatgpt_account_id: Some(format!("acct-{id}")),
        })
    }
    async fn force_refresh(&self, id: &AccountId) -> Result<Credentials, AuthError> {
        self.refreshed.fetch_add(1, Ordering::SeqCst);
        self.credentials(id).await
    }
    async fn manual_refresh(&self, id: &AccountId) -> Result<Credentials, AuthError> {
        self.force_refresh(id).await
    }
    async fn reload(&self) -> Result<(), AuthError> {
        Ok(())
    }
    async fn relogin_required(&self) -> Vec<AccountId> {
        self.relogin.lock().unwrap().iter().cloned().collect()
    }
}

#[derive(Default)]
pub struct RecordingRouter(pub Mutex<Vec<(String, Outcome)>>);
impl Router for RecordingRouter {
    fn select(&self, req: &RouteRequest, accounts: &[Account]) -> Result<Selection, RouteError> {
        accounts
            .iter()
            .find(|a| !req.exclude.contains(&a.id))
            .map(|a| Selection {
                account: a.id.clone(),
                from_binding: false,
            })
            .ok_or(RouteError::NoAccounts)
    }
    fn report(&self, _: &RouteRequest, id: &AccountId, outcome: &Outcome) {
        self.0.lock().unwrap().push((id.clone(), outcome.clone()));
    }
    fn cooling_until(&self, _: &AccountId) -> Option<std::time::SystemTime> {
        None
    }
    fn model_cooling_until(&self, _: &AccountId, _: &str) -> Option<std::time::SystemTime> {
        None
    }
    fn note_attempt(&self, _: &AccountId) {}
    fn content_ok(&self, _: &AccountId) {}
}

pub async fn serve(app: axum::Router) -> String {
    let ln = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = ln.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(
            ln,
            app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
        )
        .await
        .unwrap()
    });
    format!("127.0.0.1:{}", addr.port())
}

/// Socket I/O stays on the real clock; one configured second is 100 ms in these fixtures.
pub const TIME_UNIT: std::time::Duration = std::time::Duration::from_millis(100);

pub fn units(seconds: u32) -> std::time::Duration {
    TIME_UNIT * seconds
}

pub fn millis(milliseconds: u64) -> std::time::Duration {
    TIME_UNIT.mul_f64(milliseconds as f64 / 1000.0)
}

pub fn proxy_state(
    mut config: Config,
    accounts: Arc<dyn AccountStore>,
    router: Arc<dyn Router>,
    usage: Option<Arc<ciallopa_usage::Recorder>>,
) -> ciallopa_forward::AppState {
    config.upstream_opts.time_unit = TIME_UNIT;
    let upstream = ciallopa_upstream::UpstreamClient::new(&config).unwrap();
    upstream.pools.allow_any_cookie_host_for_tests();
    ciallopa_forward::AppState {
        accounts,
        router,
        upstream: Arc::new(upstream),
        usage,
        config: Arc::new(config.into()),
        egress: Default::default(),
    }
}

/// Each fixture has its own loopback port; share the client's expensive trust setup.
pub fn http_client() -> reqwest::Client {
    static CLIENT: std::sync::OnceLock<reqwest::Client> = std::sync::OnceLock::new();
    CLIENT.get_or_init(reqwest::Client::new).clone()
}
