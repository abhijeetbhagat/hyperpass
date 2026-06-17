use std::{collections::HashMap, io};

use crate::config::ConfigBuilder;
use crate::http::HttpProxy;
use crate::tcp::TcpProxy;
use crate::upstream::Upstream;

mod config;
mod error;
mod http;
mod loadbalance;
mod proxy;
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
            ("127.0.0.1:8090".parse().unwrap(), 2),
            ("127.0.0.1:8091".parse().unwrap(), 3),
        ]),
    );
    locs_a.insert(
        "/foo".to_owned(),
        Upstream::new(vec![
            ("127.0.0.1:8190".parse().unwrap(), 3),
            ("127.0.0.1:8191".parse().unwrap(), 2),
        ]),
    );

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
            HttpProxy::new(9080, locs_a, "certs/sample.pem", "certs/sample.rsa"),
            HttpProxy::new(9081, locs_b, "certs/sample.pem", "certs/sample.rsa"),
        ])
        .with_tcp_proxy_servers(vec![TcpProxy::new(9085, locs_c)])
        .build();

    let _ = tokio::join!(
        tcp::start_tcp_proxies(config.tcp_proxies.unwrap()),
        http::start_http_proxies(config.http_proxies.unwrap())
    );
    Ok(())
}
