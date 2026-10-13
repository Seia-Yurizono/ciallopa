//! Account storage, token refresh and login flows. **Owner: auth worker.**
//! M0: file scanning + atomic 0600 writes are real; refresh/login are stubs.
pub mod file;
pub mod login;

pub use file::{AuthFile, FileAccountStore};
