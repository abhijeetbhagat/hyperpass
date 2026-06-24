use std::{collections::HashMap, io};

use crate::config::ConfigBuilder;
use crate::http_util::HttpProxy;
use crate::shutdown::ShutdownHandler;
use crate::tcp::TcpProxy;
use crate::upstream::Upstream;
use log::*;
use std::sync::Arc;

mod config;
mod error;
mod http;
mod http_util;
mod loadbalance;
mod pool;
mod proxy;
mod rate_limiting;
mod shutdown;
mod tcp;
mod upstream;

#[tokio::main]
async fn main() -> io::Result<()> {
    env_logger::init();

    #[cfg(feature = "dev")]
    console_subscriber::init();

    let mut locs_a = HashMap::new();
    locs_a.insert(
        "/".to_owned(),
        Upstream::new(vec![
            ("127.0.0.1:8090".parse().unwrap(), 1),
            // ("127.0.0.1:8091".parse().unwrap(), 3),
        ]),
    );
    // locs_a.insert(
    //     "/foo".to_owned(),
    //     Upstream::new(vec![
    //         ("127.0.0.1:8190".parse().unwrap(), 3),
    //         ("127.0.0.1:8191".parse().unwrap(), 2),
    //     ]),
    // );

    let mut locs_b = HashMap::new();
    locs_b.insert(
        "/".to_owned(),
        Upstream::new(vec![
            ("127.0.0.1:8092".parse().unwrap(), 1),
            ("127.0.0.1:8093".parse().unwrap(), 1),
        ]),
    );

    let mut locs_c = HashMap::new();
    locs_c.insert(
        "9085".to_owned(),
        Upstream::new(vec![
            ("127.0.0.1:8290".parse().unwrap(), 1),
            ("127.0.0.1:8291".parse().unwrap(), 1),
        ]),
    );

    let config = ConfigBuilder::new()
        .with_http_proxy_servers(vec![
            HttpProxy::new(9080, 20, locs_a, "certs/sample.pem", "certs/sample.rsa"),
            // HttpProxy::new(9081, 4, locs_b, "certs/sample.pem", "certs/sample.rsa"),
        ])
        .with_tcp_proxy_servers(vec![TcpProxy::new(9085, locs_c)])
        .build();

    let shutdown_handler = Arc::new(ShutdownHandler::new());

    let _ = tokio::join!(
        async {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {
                    info!("shutting down tcp proxies");
                },
                _ = tcp::start_tcp_proxies(config.tcp_proxies.unwrap(), shutdown_handler.clone()) => {},
            }
        },
        async {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {
                    info!("shutting down http proxies");
                },
                _ = http::start_http_proxies(config.http_proxies.unwrap(), shutdown_handler.clone()) => {}
            }
        }
    );

    shutdown_handler.shutdown().await;
    info!("fin");

    Ok(())
}
