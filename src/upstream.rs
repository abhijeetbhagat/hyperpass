use std::net::SocketAddr;

/// A collection of upstream servers for load balancing
pub struct Upstream {
    pub servers: Vec<(SocketAddr, u8)>,
}

impl Upstream {
    pub fn new(servers: Vec<(SocketAddr, u8)>) -> Self {
        Self { servers }
    }
}
