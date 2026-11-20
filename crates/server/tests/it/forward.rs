//! Forwarding integration tests against a mock upstream (SSE, retry, 429 failover, 401
//! refresh, idle timeout, models cache, WebSocket relay + quota replay/close, Images API).
#![allow(clippy::unwrap_used)]
use super::support::RecordingRouter as Rec;
use super::support::{http_client, millis, proxy_state, serve, units, FixedStore as Store};
use async_trait::async_trait;
use axum::extract::ws::{Message, WebSocketUpgrade};
use axum::{
    body::Body,
    extract::State,
    http::HeaderMap,
    response::Response,
    routing::{get, post},
    Router,
};
use bytes::Bytes;
use ciallopa_core::*;
use ciallopa_forward::AppState;
use futures::{SinkExt, StreamExt};
use std::io::Write;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[macro_use]
mod scaffold;

mod admission;
mod cookies;
mod gzip;
mod images;
mod models;
mod request_id;
mod retry;
mod shutdown;
mod sse;
mod usage;
mod ws;
