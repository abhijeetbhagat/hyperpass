use std::net::SocketAddr;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

#[derive(Debug)]
pub struct LoadBalancer {
    servers: Vec<SocketAddr>,
    cur_server: AtomicUsize,
}

impl LoadBalancer {
    pub fn new(servers: Vec<SocketAddr>) -> Self {
        Self {
            servers,
            cur_server: AtomicUsize::new(0),
        }
    }

    pub fn next(&self) -> SocketAddr {
        let next_server = self
            .cur_server
            .fetch_update(Ordering::Release, Ordering::Acquire, |cur| {
                Some((cur + 1) % self.servers.len())
            })
            .unwrap();
        self.servers[next_server]
    }
}
