use std::io;

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use crate::error::HyperPassError;
use crate::loadbalance::LoadBalancer;
use crate::proxy::Proxy;
use http_body_util::{combinators::BoxBody, BodyExt};
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper::{Request, Response};
use hyper_util::rt::TokioIo;
use log::*;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::ServerConfig;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::TlsAcceptor;

type ServerBuilder = hyper::server::conn::http1::Builder;
type ClientBuilder = hyper::client::conn::http1::Builder;

pub struct Upstream {
    pub servers: Vec<SocketAddr>,
}

impl Upstream {
    pub fn new(servers: Vec<SocketAddr>) -> Self {
        Self { servers }
    }
}

pub struct HttpProxy {
    port: u16,
    locations: HashMap<String, Upstream>,
    ssl_server_cert_path: PathBuf,
    ssl_server_key_path: PathBuf,
}

impl HttpProxy {
    pub fn new(
        port: u16,
        locations: HashMap<String, Upstream>,
        ssl_server_cert_path: impl AsRef<Path>,
        ssl_server_key_path: impl AsRef<Path>,
    ) -> Self {
        Self {
            port,
            locations,
            ssl_server_cert_path: ssl_server_cert_path.as_ref().to_owned(),
            ssl_server_key_path: ssl_server_key_path.as_ref().to_owned(),
        }
    }
}

impl Proxy for HttpProxy {
    fn route(&self) {}
}

pub async fn start_http_proxies(proxies: Vec<HttpProxy>) -> Result<(), HyperPassError> {
    for proxy in proxies {
        tokio::spawn(http_listener_loop(proxy));
    }

    Ok(())
}

async fn http_listener_loop(proxy: HttpProxy) -> Result<(), HyperPassError> {
    let listener = TcpListener::bind(format!("0.0.0.0:{}", proxy.port))
        .await
        .map_err(|e| {
            error!("couldnt bind to port: {e}");
            HyperPassError::HttpServerStartError
        })?;

    let lb = Arc::new(LoadBalancer::new(
        proxy
            .locations
            .values()
            .flat_map(|u| u.servers.clone())
            .collect(),
    ));

    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let certs = CertificateDer::pem_file_iter(proxy.ssl_server_cert_path)
        .map_err(|e| {
            error!("couldn't load cert: {e}");
            HyperPassError::CertLoadError
        })?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| {
            error!("couldn't load cert: {e}");
            HyperPassError::CertLoadError
        })?;
    info!("cert loaded");
    let key = PrivateKeyDer::from_pem_file(proxy.ssl_server_key_path).map_err(|e| {
        error!("couldn't load private key: {e}");
        HyperPassError::KeyLoadError
    })?;
    info!("key loaded");
    let mut server_config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| {
            error!("{e}");
            HyperPassError::ServerConfigError
        })?;
    server_config.alpn_protocols = vec![b"http/1.1".to_vec(), b"http/1.0".to_vec(), b"h2".to_vec()];

    let tls_acceptor = TlsAcceptor::from(Arc::new(server_config));

    info!("http server listening ...");

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
    let addr = lb.next();

    let _result = ServerBuilder::new()
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
