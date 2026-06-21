use crate::error::HyperPassError;
use crate::upstream::Upstream;
use log::*;
use rustls::ServerConfig;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

pub struct HttpProxy {
    pub port: u16,
    pub locations: HashMap<String, Upstream>,
    pub ssl_server_cert_path: PathBuf,
    pub ssl_server_key_path: PathBuf,
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

pub fn tls_config(proxy: &HttpProxy) -> Result<ServerConfig, HyperPassError> {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let certs = CertificateDer::pem_file_iter(&proxy.ssl_server_cert_path)
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
    let key = PrivateKeyDer::from_pem_file(&proxy.ssl_server_key_path).map_err(|e| {
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

    Ok(server_config)
}
