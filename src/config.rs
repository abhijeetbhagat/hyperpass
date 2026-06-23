use std::path::Path;

use crate::http_util::HttpProxy;

use crate::error::HyperPassError;
use crate::tcp::TcpProxy;

enum ConfigType {
    Nginx,
    Json,
}

trait Config {
    fn parse(&self, path: impl AsRef<Path>) -> HyperPassConfig;
}

impl Config for ConfigType {
    fn parse(&self, _path: impl AsRef<Path>) -> HyperPassConfig {
        match self {
            ConfigType::Nginx => todo!(), // NginxParser::parse(path),
            ConfigType::Json => todo!(),
        }
    }
}

pub struct HyperPassConfig {
    pub http_proxies: Option<Vec<HttpProxy>>,
    pub tcp_proxies: Option<Vec<TcpProxy>>,
}

pub struct ConfigBuilder {
    http_proxies: Option<Vec<HttpProxy>>,
    tcp_proxies: Option<Vec<TcpProxy>>,
}

impl ConfigBuilder {
    pub fn new() -> Self {
        Self {
            http_proxies: None,
            tcp_proxies: None,
        }
    }

    pub fn with_http_proxy_servers(mut self, servers: Vec<HttpProxy>) -> Self {
        self.http_proxies = Some(servers);
        self
    }

    pub fn with_tcp_proxy_servers(mut self, servers: Vec<TcpProxy>) -> Self {
        self.tcp_proxies = Some(servers);
        self
    }

    pub fn build(self) -> HyperPassConfig {
        HyperPassConfig {
            http_proxies: self.http_proxies,
            tcp_proxies: self.tcp_proxies,
        }
    }
}

struct NginxParser;

impl NginxParser {
    fn parse(_path: impl AsRef<Path>) -> Result<HyperPassConfig, HyperPassError> {
        // let config = NginxDiscovery::from_config_file(path).map_err(|e| {
        //     log::error!("e");
        //     HyperPassError::ConfigReadError
        // })?;

        // let servers = config.servers();
        // let proxies = vec![];

        // for server in servers {
        //     for listen in server.listen {
        //         let mut proxy = HttpProxy {
        //             port: listen.port,
        //             locations: HashMap::new(),
        //         };
        //         proxy.locations.insert(listen, listen.)
        //         proxies.push(Box::new());
        //     }
        // }

        // let mut builder = ConfigBuilder::new()
        //     .with_http_proxy_servers(servers)
        //     .build();

        // Ok()
        todo!()
    }
}
