use std::io;

mod http;
mod tcp;

#[tokio::main]
async fn main() -> io::Result<()> {
    let _ = tokio::join!(tcp::start_tcp_proxy(), http::start_http_proxy());
    Ok(())
}
