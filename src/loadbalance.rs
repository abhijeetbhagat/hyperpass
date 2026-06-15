use std::net::SocketAddr;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

pub struct LoadBalancer {
    servers: Vec<SocketAddr>,
    cur_server: AtomicUsize,
}

impl LoadBalancer {
    pub fn new() -> Self {
        Self {
            servers: vec![
                "127.0.0.1:8090".parse().unwrap(),
                "127.0.0.1:8091".parse().unwrap(),
            ],
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
