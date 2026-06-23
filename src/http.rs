use std::io;

use std::collections::HashMap;
use std::net::SocketAddr;

use crate::error::HyperPassError;
use crate::http_util::{tls_config, HttpProxy};
use crate::loadbalance::RRLoadBalancer;
use crate::pool::ConnectionPool;
use crate::rate_limiting::RateLimiter;
use crate::shutdown::ShutdownHandler;
use dashmap::DashMap;
use futures::future::join_all;
use http_body_util::Full;
use http_body_util::{combinators::BoxBody, BodyExt};
use hyper::body::{Bytes, Incoming};
use hyper::service::service_fn;
use hyper::{Request, Response};
use hyper_util::rt::TokioIo;
use log::*;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
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
        .iter()
        .map(|(k, v)| (k.to_owned(), RRLoadBalancer::new(v.servers.clone())))
        .collect();

    info!("{:?}", lb_map);

    let lb_map = Arc::new(lb_map);

    let map: Vec<SocketAddr> = proxy
        .locations
        .into_values()
        .flat_map(|v| v.servers)
        .map(|tuple| tuple.0)
        .collect();

    let pool = Arc::new(
        ConnectionPool::new(proxy.num_conns, &map, shutdown_handler.clone())
            .await
            .map_err(|e| {
                error!("{e}");
                HyperPassError::ConnectionPoolCreationError
            })?,
    );

    let limiter_map = Arc::new(DashMap::new());
    limiter_map.insert("127.0.0.1:8090", RateLimiter::new(5, 2));
    // limiter_map.insert("127.0.0.1:8091", RateLimiter::new(5, 2));

    info!("http server listening on {} ...", proxy.port);

    loop {
        tokio::select! {
            _ = shutdown_handler.shutdown_signalled() => { break },
            pair = listener.accept() => {
                match pair {
                    Ok((sock, addr)) => {
                        let lb_map = lb_map.clone();
                        let limiter_map = limiter_map.clone();
                        let pool = pool.clone();
                        let tls_acceptor = tls_acceptor.clone();

                        shutdown_handler.spawn(async move {
                            match tls_acceptor.accept(sock).await {
                                Ok(server_tls_stream) => {
                                    if let Err(e) = handle_http_connection(lb_map, pool, server_tls_stream).await {
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
    // limiter_map: Arc<DashMap<String, RateLimiter>>,
    lb_map: Arc<HashMap<String, RRLoadBalancer>>,
    pool: Arc<ConnectionPool>,
    in_sock: tokio_rustls::server::TlsStream<TcpStream>,
) -> io::Result<()> {
    let io = TokioIo::new(in_sock);

    // TODO abhi: mutex is going to hurt the performance
    let limiter = Arc::new(Mutex::new(RateLimiter::new(5, 2)));

    let _result = ServerBuilder::new()
        .serve_connection(
            io,
            service_fn(async |req: Request<Incoming>| {
                info!("{:?}", req);

                // TODO abhi: locking is bad here
                if limiter.lock().await.process(1).is_ok() {
                    let lb = lb_map.get(&req.uri().to_string()).unwrap();
                    let addr = lb.next();
                    pool.send_request(&addr, req).await
                } else {
                    debug!("too many requests");
                    let body_data = "too many requests";
                    let full_body = Full::new(Bytes::from(body_data))
                        .map_err(|never| match never {})
                        .boxed();
                    let resp: Response<BoxBody<hyper::body::Bytes, hyper::Error>> =
                        Response::new(full_body);

                    Ok::<Response<BoxBody<hyper::body::Bytes, hyper::Error>>, HyperPassError>(
                        resp.map(|b| b.boxed()),
                    )
                }
            }),
        )
        .await;

    Ok(())
}
