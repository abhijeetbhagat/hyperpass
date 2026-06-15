use std::io;

use crate::error::HyperPassError;
use crate::loadbalance::LoadBalancer;
use http_body_util::{combinators::BoxBody, BodyExt, Full};
use hyper::body::{Bytes, Incoming};
use hyper::service::service_fn;
use hyper::{Request, Response};
use hyper_util::rt::TokioIo;
use log::*;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::ServerConfig;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::TlsAcceptor;

type ServerBuilder = hyper::server::conn::http1::Builder;
type ClientBuilder = hyper::client::conn::http1::Builder;

pub async fn start_http_proxy() -> Result<(), HyperPassError> {
    let listener = TcpListener::bind("0.0.0.0:9080").await.map_err(|e| {
        error!("couldnt bind to port: {e}");
        HyperPassError::HttpServerStartError
    })?;

    let lb = Arc::new(LoadBalancer::new());

    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let certs = CertificateDer::pem_file_iter("certs/sample.pem")
        .map_err(|e| {
            error!("couldn't load cert: {e}");
            HyperPassError::CertLoadError
        })?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| {
            error!("couldn't load cert: {e}");
            HyperPassError::CertLoadError
        })?;
    print!("cert loaded");
    let key = PrivateKeyDer::from_pem_file("certs/sample.rsa").map_err(|e| {
        error!("couldn't load private key: {e}");
        HyperPassError::KeyLoadError
    })?;
    let mut server_config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| {
            error!("{e}");
            HyperPassError::ServerConfigError
        })?;
    server_config.alpn_protocols = vec![b"http/1.1".to_vec(), b"http/1.0".to_vec(), b"h2".to_vec()];

    let tls_acceptor = TlsAcceptor::from(Arc::new(server_config));

    debug!("http server listening ...");

    while let Ok((sock, addr)) = listener.accept().await {
        let lb = lb.clone();
        let tls_acceptor = tls_acceptor.clone();

        tokio::spawn(async move {
            match tls_acceptor.accept(sock).await {
                Ok(server_tls_stream) => {
                    if let Err(e) = handle_http_connection(lb, server_tls_stream).await {
                        error!("Error handling connection from {}: {:?}", addr, e);
                    }
                }
                Err(e) => {
                    error!("TLS handshake failed for {}: {:?}", addr, e);
                }
            }
        });
    }

    Ok(())
}

async fn handle_http_connection(
    lb: Arc<LoadBalancer>,
    in_sock: tokio_rustls::server::TlsStream<TcpStream>,
) -> io::Result<()> {
    let io = TokioIo::new(in_sock);
    let addr = lb.next(); //"localhost:8090";

    let result = ServerBuilder::new()
        .serve_connection(
            io,
            service_fn(async |req: Request<Incoming>| {
                debug!("req recvd");

                let out_sock = TcpStream::connect(addr).await.map_err(|e| {
                    error!("{e}");
                    HyperPassError::UpstreamTCPConnFailed
                })?;

                let io = TokioIo::new(out_sock);

                let (mut sender, conn) = ClientBuilder::new().handshake(io).await.map_err(|e| {
                    error!("{e}");
                    HyperPassError::UpstreamHandshakeFailed
                })?;

                tokio::spawn(async {
                    if let Err(e) = conn.await {
                        debug!("err: {}", e);
                    }
                });

                let resp = sender.send_request(req).await.map_err(|e| {
                    error!("{e}");
                    HyperPassError::UpstreamRequestError
                })?;

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
    debug!("req recvd");

    let out_sock = TcpStream::connect("localhost:8090").await.map_err(|e| {
        error!("{e}");
        HyperPassError::UpstreamTCPConnFailed
    })?;
    let io = TokioIo::new(out_sock);
    let (mut sender, conn) = ClientBuilder::new().handshake(io).await.map_err(|e| {
        error!("{e}");
        HyperPassError::UpstreamHandshakeFailed
    })?;
    tokio::spawn(async {
        if let Err(e) = conn.await {
            debug!("err: {}", e);
        }
    });

    let resp = sender.send_request(req).await.map_err(|e| {
        error!("{e}");
        HyperPassError::UpstreamRequestError
    })?;

    Ok(resp.map(|b| b.boxed()))
}
