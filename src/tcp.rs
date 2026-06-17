use crate::error::HyperPassError;
use crate::loadbalance::RRLoadBalancer;
use crate::upstream::Upstream;
use futures::future::join_all;
use log::debug;
use log::*;
use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::copy_bidirectional;
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

pub struct TcpProxy {
    port: u16,
    locations: HashMap<String, Upstream>,
}

impl TcpProxy {
    pub fn new(port: u16, locations: HashMap<String, Upstream>) -> Self {
        Self { port, locations }
    }
}

pub async fn start_tcp_proxies(proxies: Vec<TcpProxy>) -> io::Result<()> {
    let handles: Vec<JoinHandle<Result<(), HyperPassError>>> = proxies
        .into_iter()
        .map(|proxy| tokio::spawn(tcp_listener_loop(proxy)))
        .collect();

    join_all(handles).await;

    Ok(())
}

async fn tcp_listener_loop(proxy: TcpProxy) -> Result<(), HyperPassError> {
    let listener = TcpListener::bind(format!("0.0.0.0:{}", proxy.port))
        .await
        .map_err(|e| {
            error!("couldnt bind to port: {e}");
            HyperPassError::TcpServerBindError
        })?;

    info!("tcp server listening on {} ...", proxy.port);
    let TcpProxy {
        port,
        mut locations,
    } = proxy;

    let servers: Vec<(SocketAddr, u8)> = locations.remove(&port.to_string()).unwrap().servers;
    let lb = Arc::new(RRLoadBalancer::new(servers));

    while let Ok((sock, _addr)) = listener.accept().await {
        let lb = lb.clone();
        tokio::spawn(async move { handle_connection(lb, sock).await });
    }

    Ok(())
}

async fn handle_connection(lb: Arc<RRLoadBalancer>, mut in_sock: TcpStream) -> io::Result<()> {
    debug!("{:?}", in_sock.peer_addr());
    let addr = lb.next();

    let mut out_sock = TcpStream::connect(addr).await?;
    copy_bidirectional(&mut in_sock, &mut out_sock).await?;
    Ok(())
}
