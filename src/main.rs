use std::io;

use http_body_util::{combinators::BoxBody, BodyExt, Full};
use hyper::body::{Bytes, Incoming};
use hyper::service::service_fn;
use hyper::{Request, Response};
use hyper_util::rt::TokioIo;
use tokio::io::copy_bidirectional;
use tokio::net::{TcpListener, TcpStream};

// type ServerBuilder = hyper::server::conn::http2::Builder<TokioExecutor>;
// type ClientBuilder = hyper::client::conn::http2::Builder<TokioExecutor>;

type ServerBuilder = hyper::server::conn::http1::Builder;
type ClientBuilder = hyper::client::conn::http1::Builder;

enum HyperPassError {
    UpstreamConnectError,
    UpstreamRequestError,
}

#[tokio::main]
async fn main() -> io::Result<()> {
    let _ = tokio::join!(start_tcp_proxy(), start_http_proxy());
    Ok(())
}

async fn start_tcp_proxy() -> io::Result<()> {
    let listener = TcpListener::bind("0.0.0.0:8080").await?;
    println!("tcp server listening ...");
    while let Ok((sock, addr)) = listener.accept().await {
        tokio::spawn(async move { handle_connection(sock).await });
    }

    Ok(())
}

async fn handle_connection(mut in_sock: TcpStream) -> io::Result<()> {
    println!("{:?}", in_sock.peer_addr());
    let mut out_sock = TcpStream::connect("0.0.0.0:8081").await?;
    copy_bidirectional(&mut in_sock, &mut out_sock).await?;
    Ok(())
}

async fn start_http_proxy() -> io::Result<()> {
    let listener = TcpListener::bind("0.0.0.0:9080").await?;
    println!("http server listening ...");
    while let Ok((sock, addr)) = listener.accept().await {
        tokio::spawn(async move { handle_http_connection(sock).await });
    }

    Ok(())
}

async fn handle_http_connection(in_sock: TcpStream) -> io::Result<()> {
    let io = TokioIo::new(in_sock);
    let result = ServerBuilder::new()
        .serve_connection(io, service_fn(service))
        .await;
    Ok(())
}

async fn service(
    req: Request<Incoming>,
) -> Result<Response<BoxBody<Bytes, hyper::Error>>, hyper::Error> {
    println!("req recvd");

    let out_sock = TcpStream::connect("localhost:8090").await.unwrap();
    let io = TokioIo::new(out_sock);
    let (mut sender, conn) = ClientBuilder::new().handshake(io).await?;

    // .map_err(|e| HyperPassError::UpstreamConnectError)?;
    tokio::spawn(async {
        if let Err(e) = conn.await {
            println!("err: {}", e);
        }
    });

    let resp = sender.send_request(req).await?;
    // .map_err(|e| HyperPassError::UpstreamRequestError)?;

    Ok(resp.map(|b| b.boxed()))
}
