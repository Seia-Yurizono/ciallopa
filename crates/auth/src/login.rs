//! Login flows (M1, auth worker): each ends by `file::write_auth_file`.
//! - device code: auth.openai.com/api/accounts/deviceauth/{usercode,token} (codex-rs login/src/device_code_auth.rs)
//! - paste callback URL: print PKCE auth URL, user pastes `http://localhost:1455/auth/callback?code=..&state=..`
//! - PKCE 1455: listener on 127.0.0.1:1455 only, 5 min lifetime
//! - import: ~/.codex/auth.json
use cpa_lite_core::AuthError;
use std::path::Path;

pub async fn device_code(_auth_dir: &Path) -> Result<String, AuthError> {
    Err(AuthError::Unimplemented)
}
pub async fn paste_callback(_auth_dir: &Path) -> Result<String, AuthError> {
    Err(AuthError::Unimplemented)
}
pub async fn pkce_localhost(_auth_dir: &Path) -> Result<String, AuthError> {
    Err(AuthError::Unimplemented)
}
pub async fn import_codex_auth_json(_src: &Path, _auth_dir: &Path) -> Result<String, AuthError> {
    Err(AuthError::Unimplemented)
}
