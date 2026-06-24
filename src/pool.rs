use dashmap::DashMap;
use std::net::SocketAddr;

use crate::{error::HyperPassError, shutdown::ShutdownHandler};
use crossbeam::queue::ArrayQueue;
use http_body_util::{BodyExt, combinators::BoxBody};
use hyper::{Request, Response, body::Incoming, client::conn::http1::SendRequest};
use hyper_util::rt::TokioIo;
use log::*;
use std::sync::Arc;
use tokio::net::TcpStream;

type ClientBuilder = hyper::client::conn::http1::Builder;

pub struct InnerConnection(SendRequest<Incoming>);

pub struct HyperPassConnection {
    addr: SocketAddr,
    inner: Option<InnerConnection>,
    pool: Arc<InnerPool>,
}

impl Drop for HyperPassConnection {
    /// returns connection back to the pool
    fn drop(&mut self) {
        if let Some(inner) = self.inner.take() {
            debug!("adding conn back to pool");
            let _ = self.pool.conns.get_mut(&self.addr).unwrap().push(inner);
        }
    }
}

#[derive(Clone)]
pub struct ConnectionPool {
    inner: Arc<InnerPool>,
}

struct InnerPool {
    conns: DashMap<SocketAddr, ArrayQueue<InnerConnection>>,
}

impl ConnectionPool {
    pub async fn new(
        num_conns: u8,
        addrs: &[SocketAddr],
        shutdown_handler: Arc<ShutdownHandler>,
    ) -> Result<Self, HyperPassError> {
        let conns = DashMap::new();

        for addr in addrs {
            let queue = ArrayQueue::new(num_conns as usize);

            for _ in 0..num_conns {
                let out_sock = TcpStream::connect(addr).await.map_err(|e| {
                    error!("failed to connect to {addr}: {e}");
                    HyperPassError::UpstreamTCPConnFailed
                })?;

                let io = TokioIo::new(out_sock);

                let (sender, conn) = ClientBuilder::new().handshake(io).await.map_err(|e| {
                    error!("failed to handshake with {addr}: {e}");
                    HyperPassError::UpstreamHandshakeFailed
                })?;

                shutdown_handler.spawn(async {
                    if let Err(e) = conn.await {
                        debug!("conn awaiting over. err: {}", e);
                    }
                });

                let _ = queue.push(InnerConnection(sender));

                // pool.add_conn(InnerConnection(sender));
            }

            debug!("adding {} values", queue.len());

            conns.insert(addr.to_owned(), queue);
        }

        let inner = InnerPool { conns };
        let pool = Self {
            inner: Arc::new(inner),
        };

        Ok(pool)
    }

    // fn add_conn(&self, conn: InnerConnection) {
    //     let _ = self.inner.conns.push(conn);
    // }

    fn get_conn(&self, addr: &SocketAddr) -> Option<HyperPassConnection> {
        if let Some(queue) = self.inner.conns.get_mut(addr) {
            queue.pop().map(|conn| {
                Some(HyperPassConnection {
                    addr: addr.to_owned(),
                    inner: Some(conn),
                    pool: self.inner.clone(),
                })
            })?
        } else {
            None
        }
    }

    pub async fn send_request(
        &self,
        addr: &SocketAddr,
        req: Request<Incoming>,
    ) -> Result<Response<BoxBody<hyper::body::Bytes, hyper::Error>>, HyperPassError> {
        debug!("pool len: {}", self.inner.conns.len());

        if let Some(mut conn) = self.get_conn(addr) {
            let resp = conn
                .inner
                .as_mut()
                .unwrap()
                .0
                .send_request(req)
                .await
                .map_err(|e| {
                    error!("{e}");
                    HyperPassError::UpstreamRequestError
                })?;

            Ok::<Response<BoxBody<hyper::body::Bytes, hyper::Error>>, HyperPassError>(
                resp.map(|b| b.boxed()),
            )
        } else {
            Err(HyperPassError::ConnectionPoolEmptyError)
        }
    }
}
