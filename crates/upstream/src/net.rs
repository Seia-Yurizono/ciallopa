//! Direct TCP dialing for the WebSocket path (reqwest has its own connector).
//!
//! `ipv6 = false`: IPv4 addresses only. `ipv6 = true`: Happy Eyeballs (RFC 8305 style, as
//! codex-rs): addresses interleaved IPv6-first, a new attempt starts every
//! [`ATTEMPT_DELAY`] or as soon as the previous one fails, the first connection wins and the
//! others are dropped.
use std::net::SocketAddr;
use std::time::Duration;
use tokio::net::TcpStream;

/// Connection attempt delay (codex-rs uses 250 ms; RFC 8305 recommends 250 ms).
pub const ATTEMPT_DELAY: Duration = Duration::from_millis(250);

/// `[::1]` → `::1` (URI hosts keep the brackets).
fn bare(host: &str) -> &str {
    host.strip_prefix('[')
        .and_then(|h| h.strip_suffix(']'))
        .unwrap_or(host)
}

/// `host:port` / `[v6]:port` → (host, port).
pub fn split_authority(a: &str) -> Option<(&str, u16)> {
    let (h, p) = a.rsplit_once(':')?;
    Some((bare(h), p.parse().ok()?))
}

/// Resolve and order the candidates: IPv4 only, or IPv6 / IPv4 interleaved, IPv6 first.
pub async fn resolve(host: &str, port: u16, ipv6: bool) -> std::io::Result<Vec<SocketAddr>> {
    let all: Vec<SocketAddr> = tokio::net::lookup_host((bare(host), port)).await?.collect();
    let (v6, v4): (Vec<_>, Vec<_>) = all.into_iter().partition(SocketAddr::is_ipv6);
    if !ipv6 {
        if v4.is_empty() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::AddrNotAvailable,
                format!("{host}: no IPv4 address (ipv6 = false)"),
            ));
        }
        return Ok(v4);
    }
    let mut out = Vec::with_capacity(v6.len() + v4.len());
    let (mut a, mut b) = (v6.into_iter(), v4.into_iter());
    loop {
        match (a.next(), b.next()) {
            (None, None) => break,
            (x, y) => out.extend(x.into_iter().chain(y)),
        }
    }
    Ok(out)
}

pub async fn dial(host: &str, port: u16, ipv6: bool) -> std::io::Result<TcpStream> {
    let addrs = resolve(host, port, ipv6).await?;
    race(&addrs, ATTEMPT_DELAY).await
}

/// Staggered connection attempts over `addrs` (in order); first success wins.
pub async fn race(addrs: &[SocketAddr], delay: Duration) -> std::io::Result<TcpStream> {
    let mut set = tokio::task::JoinSet::new();
    let mut next = 0;
    let mut errors: Vec<String> = vec![];
    let timer = tokio::time::sleep(Duration::ZERO);
    tokio::pin!(timer);
    loop {
        tokio::select! {
            _ = &mut timer, if next < addrs.len() => {
                let a = addrs[next];
                next += 1;
                set.spawn(async move { (a, TcpStream::connect(a).await) });
                timer.as_mut().reset(tokio::time::Instant::now() + delay);
            }
            Some(r) = set.join_next(), if !set.is_empty() => match r {
                Ok((_, Ok(s))) => return Ok(s), // dropping `set` aborts the other attempts
                Ok((a, Err(e))) => {
                    errors.push(format!("{a}: {e}"));
                    // A failure starts the next attempt at once.
                    timer.as_mut().reset(tokio::time::Instant::now());
                }
                Err(e) => errors.push(e.to_string()),
            },
            else => break,
        }
        if next >= addrs.len() && set.is_empty() {
            break;
        }
    }
    Err(std::io::Error::new(
        std::io::ErrorKind::ConnectionRefused,
        if errors.is_empty() {
            "no address to connect to".to_string()
        } else {
            errors.join("; ")
        },
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn authority() {
        assert_eq!(split_authority("[::1]:1080"), Some(("::1", 1080)));
        assert_eq!(split_authority("h.example:3128"), Some(("h.example", 3128)));
        assert_eq!(split_authority("nope"), None);
    }

    #[tokio::test]
    async fn ipv4_only_and_interleaving() {
        let v4 = resolve("127.0.0.1", 1, false).await.unwrap();
        assert!(v4.iter().all(SocketAddr::is_ipv4));
        assert!(
            resolve("::1", 1, false).await.is_err(),
            "no v6 when ipv6 = false"
        );
        assert!(resolve("[::1]", 1, true).await.unwrap()[0].is_ipv6());
    }

    /// An attempt that never answers (blackholed) does not hold up the next one longer than
    /// the attempt delay; the first success wins.
    #[tokio::test]
    async fn stalled_first_address_falls_through_after_delay() {
        let ln = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let good = ln.local_addr().unwrap();
        // 10.255.255.1 is unroutable in test sandboxes: its SYN goes nowhere (or fails fast).
        let stall: SocketAddr = "10.255.255.1:9".parse().unwrap();
        let t = std::time::Instant::now();
        let s = race(&[stall, good], Duration::from_millis(100))
            .await
            .unwrap();
        assert_eq!(s.peer_addr().unwrap(), good);
        assert!(t.elapsed() < Duration::from_secs(2), "{:?}", t.elapsed());
        // Every address failing gives one combined error.
        let dead: SocketAddr = {
            let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            l.local_addr().unwrap()
        };
        let e = race(&[dead, dead], Duration::from_millis(10))
            .await
            .unwrap_err();
        assert_eq!(e.to_string().matches(&dead.to_string()).count(), 2, "{e}");
    }
}
