use std::{collections::HashMap, io};

use crate::config::ConfigBuilder;
use crate::http::{HttpProxy, Upstream};

mod config;
mod error;
mod http;
mod loadbalance;
mod proxy;
mod tcp;

#[tokio::main]
async fn main() -> io::Result<()> {
    env_logger::init();

    let mut locs = HashMap::new();
    locs.insert(
        "/".to_owned(),
        Upstream::new(vec![
            "127.0.0.1:8090".parse().unwrap(),
            "127.0.0.1:8091".parse().unwrap(),
        ]),
    );
    let config = ConfigBuilder::new()
        .with_http_proxy_servers(vec![HttpProxy::new(
            9080,
            locs,
            "certs/sample.pem",
            "certs/sample.rsa",
        )])
        .build();
    let _ = tokio::join!(
        tcp::start_tcp_proxy(),
        http::start_http_proxy(config.http_proxies.unwrap())
    );
    Ok(())
}
