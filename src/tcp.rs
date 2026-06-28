use crate::error::HyperPassError;
use crate::loadbalance::RRLoadBalancer;
use crate::shutdown::ShutdownHandler;
use crate::upstream::Upstream;
use futures::future::join_all;
use hickory_resolver::Resolver;
use log::debug;
use log::*;
use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt, copy_bidirectional};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

pub struct TcpProxy {
    port: u16,
    locations: HashMap<String, Upstream>,
    is_tunneled: bool,
}

impl TcpProxy {
    pub fn new(port: u16, locations: HashMap<String, Upstream>, is_tunneled: bool) -> Self {
        Self {
            port,
            locations,
            is_tunneled,
        }
    }
}

pub async fn start_tcp_proxies(
    proxies: Vec<TcpProxy>,
    // cancellation_token: CancellationToken,
    shutdown_handler: Arc<ShutdownHandler>,
) -> io::Result<()> {
    let handles: Vec<JoinHandle<Result<(), HyperPassError>>> = proxies
        .into_iter()
        .map(|proxy| {
            let shutdown_handler = shutdown_handler.clone();
            shutdown_handler.spawn(tcp_listener_loop(proxy, shutdown_handler.clone()))
        })
        .collect();

    join_all(handles).await;

    Ok(())
}

async fn tcp_listener_loop(
    proxy: TcpProxy,
    shutdown_handler: Arc<ShutdownHandler>,
) -> Result<(), HyperPassError> {
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
        is_tunneled,
    } = proxy;

    if !is_tunneled {
        let servers: Vec<(SocketAddr, u8)> = locations.remove(&port.to_string()).unwrap().servers;
        let lb = Arc::new(RRLoadBalancer::new(servers));

        loop {
            tokio::select! {
                _ = shutdown_handler.shutdown_signalled() => { break },
                pair = listener.accept() => {
                    match pair {
                        Ok((sock, _addr)) => {
                            let lb = lb.clone();
                            let shutdown_handler = shutdown_handler.clone();
                            let shutdown_handler_clone = shutdown_handler.clone();
                            shutdown_handler_clone
                                .spawn(async move { handle_connection(lb, sock, shutdown_handler.clone()).await });
                        }
                        _ => break
                    }
                }
            }
        }
    } else {
        loop {
            tokio::select! {
                _ = shutdown_handler.shutdown_signalled() => { break },
                pair = listener.accept() => {
                    match pair {
                        Ok((sock, _addr)) => {
                            let shutdown_handler = shutdown_handler.clone();
                            let shutdown_handler_clone = shutdown_handler.clone();
                            shutdown_handler_clone
                                .spawn(async move { handle_tunnel_connection(sock, shutdown_handler.clone()).await });
                        }
                        _ => break
                    }
                }
            }
        }
    }

    Ok(())
}

async fn handle_connection(
    lb: Arc<RRLoadBalancer>,
    mut in_sock: TcpStream,
    shutdown_handler: Arc<ShutdownHandler>,
) -> io::Result<()> {
    debug!("{:?}", in_sock.peer_addr());
    let addr = lb.next();

    let mut out_sock = TcpStream::connect(addr).await?;
    tokio::select! {
        _ = copy_bidirectional(&mut in_sock, &mut out_sock) => {},
        _ = shutdown_handler.shutdown_signalled() => {
            info!("relaying cancelled. closing sockets ...");
            let _ = in_sock.shutdown().await;
            let _ = out_sock.shutdown().await;
            info!("sockets closed");
        }
    }
    Ok(())
}

async fn handle_tunnel_connection(
    mut in_sock: TcpStream,
    shutdown_handler: Arc<ShutdownHandler>,
) -> io::Result<()> {
    debug!("{:?}", in_sock.peer_addr());

    let mut buf = [0u8; 2048];
    let bytes_read = in_sock.read(&mut buf).await?;
    let mut headers = [httparse::EMPTY_HEADER; 64];
    let mut req = httparse::Request::new(&mut headers);
    let _ = req.parse(&buf[..bytes_read]).unwrap();

    if let Some("CONNECT") = req.method
        && let Some(url) = req.path
    {
        let resolver = Resolver::builder_tokio().unwrap().build().unwrap();
        let parts: Vec<&str> = url.split(":").collect();
        let response = resolver.lookup_ip(parts[0]).await.unwrap();
        let ip = response.iter().next().unwrap();
        let mut out_sock = TcpStream::connect((ip, parts[1].parse().unwrap())).await?;
        in_sock
            .write_all(b"HTTP/1.1 200 Connection Established\r\n\r\n")
            .await?;

        tokio::select! {
            _ = copy_bidirectional(&mut in_sock, &mut out_sock) => {},
            _ = shutdown_handler.shutdown_signalled() => {
                info!("relaying cancelled. closing sockets ...");
                let _ = in_sock.shutdown().await;
                let _ = out_sock.shutdown().await;
                info!("sockets closed");
            }
        }
    }
    Ok(())
}
