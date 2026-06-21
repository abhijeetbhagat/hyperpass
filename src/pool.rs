use std::net::SocketAddr;

use crate::{error::HyperPassError, shutdown::ShutdownHandler};
use crossbeam::queue::ArrayQueue;
use http_body_util::{combinators::BoxBody, BodyExt};
use hyper::{body::Incoming, client::conn::http1::SendRequest, Request, Response};
use hyper_util::{client::legacy::connect::Connection, rt::TokioIo};
use log::*;
use std::sync::{Arc, Weak};
use tokio::net::TcpStream;

type ClientBuilder = hyper::client::conn::http1::Builder;

pub struct InnerConnection(SendRequest<Incoming>);

pub struct HyperPassConnection {
    inner: Option<InnerConnection>,
    pool: Weak<InnerPool>,
}

impl Drop for HyperPassConnection {
    /// returns connection back to the pool
    fn drop(&mut self) {
        if let Some(inner) = self.inner.take() {
            let pool = self.pool.upgrade().unwrap();
            debug!("adding conn back to pool");
            let _ = pool.conns.push(inner);
        }
    }
}

#[derive(Clone)]
pub struct ConnectionPool {
    inner: Arc<InnerPool>,
}

struct InnerPool {
    conns: ArrayQueue<InnerConnection>,
}

impl ConnectionPool {
    pub async fn new(
        addrs: &[SocketAddr],
        shutdown_handler: Arc<ShutdownHandler>,
    ) -> Result<Self, HyperPassError> {
        let conns = ArrayQueue::new(2);
        let inner = InnerPool { conns };
        let pool = Self {
            inner: Arc::new(inner),
        };

        for addr in addrs {
            let out_sock = TcpStream::connect(addr).await.map_err(|e| {
                error!("{e}");
                HyperPassError::UpstreamTCPConnFailed
            })?;

            let io = TokioIo::new(out_sock);

            let (sender, conn) = ClientBuilder::new().handshake(io).await.map_err(|e| {
                error!("{e}");
                HyperPassError::UpstreamHandshakeFailed
            })?;

            shutdown_handler.spawn(async {
                if let Err(e) = conn.await {
                    debug!("err: {}", e);
                }
            });

            pool.add_conn(InnerConnection(sender));
        }

        Ok(pool)
    }

    fn add_conn(&self, conn: InnerConnection) {
        let _ = self.inner.conns.push(conn);
    }

    fn get_conn(&self) -> Option<HyperPassConnection> {
        self.inner.conns.pop().map(|conn| {
            Some(HyperPassConnection {
                inner: Some(conn),
                pool: Arc::downgrade(&self.inner),
            })
        })?
    }

    pub async fn send_request(
        &self,
        req: Request<Incoming>,
    ) -> Result<Response<BoxBody<hyper::body::Bytes, hyper::Error>>, HyperPassError> {
        debug!("pool len: {}", self.inner.conns.len());

        if let Some(mut conn) = self.get_conn() {
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
