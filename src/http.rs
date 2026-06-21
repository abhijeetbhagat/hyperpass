use std::io;

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use crate::error::HyperPassError;
use crate::http_util::{HttpProxy, tls_config};
use crate::loadbalance::RRLoadBalancer;
use crate::pool::ConnectionPool;
use crate::shutdown::ShutdownHandler;
use futures::future::join_all;
use http_body_util::{BodyExt, combinators::BoxBody};
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper::{Request, Response};
use hyper_util::rt::TokioIo;
use log::*;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;
use tokio_rustls::TlsAcceptor;

type ServerBuilder = hyper::server::conn::http1::Builder;

pub async fn start_http_proxies(
    proxies: Vec<HttpProxy>,
    shutdown_handler: Arc<ShutdownHandler>,
) -> Result<(), HyperPassError> {
    let handles: Vec<JoinHandle<Result<(), HyperPassError>>> = proxies
        .into_iter()
        .map(|proxy| {
            #[cfg(feature = "dev")]
            {
                tokio::task::Builder::new()
                    .name("listener loop")
                    .spawn(http_listener_loop(proxy))
            }

            #[cfg(not(feature = "dev"))]
            let shutdown_handler = shutdown_handler.clone();
            shutdown_handler.spawn(http_listener_loop(proxy, shutdown_handler.clone()))
        })
        .collect();

    join_all(handles).await;

    Ok(())
}

async fn http_listener_loop(
    proxy: HttpProxy,
    shutdown_handler: Arc<ShutdownHandler>,
) -> Result<(), HyperPassError> {
    let listener = TcpListener::bind(format!("0.0.0.0:{}", proxy.port))
        .await
        .map_err(|e| {
            error!("couldnt bind to port: {e}");
            HyperPassError::HttpServerStartError
        })?;

    let tls_acceptor = TlsAcceptor::from(Arc::new(tls_config(&proxy)?));

    let lb_map: HashMap<String, RRLoadBalancer> = proxy
        .locations
        .into_iter()
        .map(|(k, v)| (k, RRLoadBalancer::new(v.servers)))
        .collect();

    info!("{:?}", lb_map);

    let lb_map = Arc::new(lb_map);

    let pool = Arc::new(
        ConnectionPool::new(
            &[
                "127.0.0.1:8090".parse().unwrap(),
                "127.0.0.1:8091".parse().unwrap(),
            ],
            shutdown_handler.clone(),
        )
        .await
        .map_err(|e| {
            error!("{e}");
            HyperPassError::ConnectionPoolCreationError
        })?,
    );

    info!("http server listening on {} ...", proxy.port);

    loop {
        tokio::select! {
            _ = shutdown_handler.shutdown_signalled() => { break },
            pair = listener.accept() => {
                match pair {
                    Ok((sock, addr)) => {
                        let lb_map = lb_map.clone();
                        let pool = pool.clone();
                        let tls_acceptor = tls_acceptor.clone();

                        shutdown_handler.spawn(async move {
                            match tls_acceptor.accept(sock).await {
                                Ok(server_tls_stream) => {
                                    if let Err(e) = handle_http_connection(pool, server_tls_stream).await {
                                        error!("Error handling connection from {}: {:?}", addr, e);
                                    }
                                }
                                Err(e) => {
                                    error!("TLS handshake failed for {}: {:?}", addr, e);
                                }
                            }
                        });
                    }
                    _ => break
                }
            }
        }
    }

    Ok(())
}

async fn handle_http_connection(
    pool: Arc<ConnectionPool>,
    in_sock: tokio_rustls::server::TlsStream<TcpStream>,
) -> io::Result<()> {
    let io = TokioIo::new(in_sock);

    let _result = ServerBuilder::new()
        .serve_connection(
            io,
            service_fn(async |req: Request<Incoming>| {
                info!("{:?}", req);
                pool.send_request(req).await
            }),
        )
        .await;

    Ok(())
}
