use log::debug;
use std::io;
use tokio::io::copy_bidirectional;
use tokio::net::{TcpListener, TcpStream};
use log::*;

pub async fn start_tcp_proxy() -> io::Result<()> {
    let listener = TcpListener::bind("0.0.0.0:8080").await?;
    info!("tcp server listening ...");
    while let Ok((sock, _addr)) = listener.accept().await {
        tokio::spawn(async move { handle_connection(sock).await });
    }

    Ok(())
}

async fn handle_connection(mut in_sock: TcpStream) -> io::Result<()> {
    debug!("{:?}", in_sock.peer_addr());
    let mut out_sock = TcpStream::connect("0.0.0.0:8081").await?;
    copy_bidirectional(&mut in_sock, &mut out_sock).await?;
    Ok(())
}
