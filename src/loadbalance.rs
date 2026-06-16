use std::net::SocketAddr;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

#[derive(Debug)]
/// A Simple & Weighted Round Robin load balancer
pub struct RRLoadBalancer {
    servers: Vec<SocketAddr>,
    cur_server: AtomicUsize,
}

impl RRLoadBalancer {
    pub fn new(servers: Vec<(SocketAddr, u8)>) -> Self {
        let servers = servers
            .into_iter()
            .flat_map(|t| vec![t.0; t.1 as usize])
            .collect();
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
