use std::io;
use std::net::SocketAddr;
use std::sync::atomic::AtomicUsize;

use http_body_util::{BodyExt, Full, combinators::BoxBody};
use hyper::body::{Bytes, Incoming};
use hyper::service::service_fn;
use hyper::{Request, Response};
use hyper_util::rt::TokioIo;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use tokio::net::{TcpListener, TcpStream};

type ServerBuilder = hyper::server::conn::http1::Builder;
type ClientBuilder = hyper::client::conn::http1::Builder;

#[derive(thiserror::Error, Debug)]
enum HyperPassError {
    #[error("failed to connect to upstream server")]
    UpstreamConnectError,
    #[error("failed to send request to upstream server")]
    UpstreamRequestError,
    #[error("failed to open tcp connection to upstream server")]
    UpstreamTCPConnFailed,
    #[error("failed to handshake with upstream server")]
    UpstreamHandshakeFailed,
}

struct LoadBalancer {
    servers: Vec<SocketAddr>,
    cur_server: AtomicUsize,
}

impl LoadBalancer {
    fn new() -> Self {
        Self {
            servers: vec![
                "127.0.0.1:8090".parse().unwrap(),
                "127.0.0.1:8091".parse().unwrap(),
            ],
            cur_server: AtomicUsize::new(0),
        }
    }

    fn next(&self) -> SocketAddr {
        let next_server = self
            .cur_server
            .fetch_update(Ordering::Release, Ordering::Acquire, |cur| {
                Some((cur + 1) % self.servers.len())
            })
            .unwrap();
        self.servers[next_server]
    }
}

pub async fn start_http_proxy() -> io::Result<()> {
    let listener = TcpListener::bind("0.0.0.0:9080").await?;
    println!("http server listening ...");

    let lb = Arc::new(LoadBalancer::new());

    while let Ok((sock, addr)) = listener.accept().await {
        let lb = lb.clone();
        tokio::spawn(async move { handle_http_connection(lb, sock).await });
    }

    Ok(())
}

async fn handle_http_connection(lb: Arc<LoadBalancer>, in_sock: TcpStream) -> io::Result<()> {
    let io = TokioIo::new(in_sock);
    let addr = lb.next(); //"localhost:8090";

    let result = ServerBuilder::new()
        .serve_connection(
            io,
            service_fn(async |req: Request<Incoming>| {
                println!("req recvd");

                let out_sock = TcpStream::connect(addr)
                    .await
                    .map_err(|e| HyperPassError::UpstreamTCPConnFailed)?;

                let io = TokioIo::new(out_sock);

                let (mut sender, conn) = ClientBuilder::new()
                    .handshake(io)
                    .await
                    .map_err(|e| HyperPassError::UpstreamHandshakeFailed)?;

                tokio::spawn(async {
                    if let Err(e) = conn.await {
                        println!("err: {}", e);
                    }
                });

                let resp = sender
                    .send_request(req)
                    .await
                    .map_err(|e| HyperPassError::UpstreamRequestError)?;

                Ok::<Response<BoxBody<hyper::body::Bytes, hyper::Error>>, HyperPassError>(
                    resp.map(|b| b.boxed()),
                )
            }),
        )
        .await;
    Ok(())
}

async fn service(
    req: Request<Incoming>,
) -> Result<Response<BoxBody<Bytes, hyper::Error>>, HyperPassError> {
    println!("req recvd");

    let out_sock = TcpStream::connect("localhost:8090")
        .await
        .map_err(|e| HyperPassError::UpstreamTCPConnFailed)?;
    let io = TokioIo::new(out_sock);
    let (mut sender, conn) = ClientBuilder::new()
        .handshake(io)
        .await
        .map_err(|e| HyperPassError::UpstreamHandshakeFailed)?;
    tokio::spawn(async {
        if let Err(e) = conn.await {
            println!("err: {}", e);
        }
    });

    let resp = sender
        .send_request(req)
        .await
        .map_err(|e| HyperPassError::UpstreamRequestError)?;

    Ok(resp.map(|b| b.boxed()))
}
