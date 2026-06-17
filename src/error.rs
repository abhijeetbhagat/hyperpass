#[derive(thiserror::Error, Debug)]
pub enum HyperPassError {
    #[error("failed to connect to upstream server")]
    UpstreamConnectError,
    #[error("failed to send request to upstream server")]
    UpstreamRequestError,
    #[error("failed to open tcp connection to upstream server")]
    UpstreamTCPConnFailed,
    #[error("failed to handshake with upstream server")]
    UpstreamHandshakeFailed,
    #[error("cert load error")]
    CertLoadError,
    #[error("key load error")]
    KeyLoadError,
    #[error("server config error")]
    ServerConfigError,
    #[error("http server start error")]
    HttpServerStartError,
    #[error("tls handshake error")]
    TlsHandshakeError,
    #[error("config read error")]
    ConfigReadError,

    #[error("tcp server start error")]
    TcpServerBindError,
}
