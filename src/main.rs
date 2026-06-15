use std::io;

mod error;
mod http;
mod loadbalance;
mod tcp;

#[tokio::main]
async fn main() -> io::Result<()> {
    let _ = tokio::join!(tcp::start_tcp_proxy(), http::start_http_proxy());
    Ok(())
}
