//! Shared types, traits and config for ciallopa. Owned by the lead; changes to
//! anything public here must be agreed in docs/INTERFACES.md first.
pub mod config;
pub mod error;
pub mod fingerprint;
pub mod logging;
pub mod settings;
pub mod traits;
pub mod types;

pub use config::Config;
pub use error::{AuthError, RouteError};
pub use traits::{AccountStore, Router};
pub use types::*;
