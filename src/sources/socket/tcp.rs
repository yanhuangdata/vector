use bytes::Bytes;
use chrono::Utc;
use codecs::decoding::{DeserializerConfig, FramingConfig};
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;

use crate::{
    codecs::Decoder,
    config::log_schema,
    event::Event,
    serde::default_decoding,
    sources::util::{SocketListenAddr, TcpNullAcker, TcpSource},
    tcp::TcpKeepaliveConfig,
    tls::TlsEnableableConfig,
};

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct TcpConfig {
    address: SocketListenAddr,
    keepalive: Option<TcpKeepaliveConfig>,
    max_length: Option<usize>,
    #[serde(default = "default_shutdown_timeout_secs")]
    shutdown_timeout_secs: u64,
    host_key: Option<String>,
    port_key: Option<String>,
    tls: Option<TlsEnableableConfig>,
    receive_buffer_bytes: Option<usize>,
    framing: Option<FramingConfig>,
    #[serde(default = "default_decoding")]
    decoding: DeserializerConfig,
    pub connection_limit: Option<u32>,
}

const fn default_shutdown_timeout_secs() -> u64 {
    30
}

impl TcpConfig {
    pub fn from_address(address: SocketListenAddr) -> Self {
        Self {
            address,
            keepalive: None,
            max_length: Some(crate::serde::default_max_length()),
            shutdown_timeout_secs: default_shutdown_timeout_secs(),
            host_key: None,
            port_key: Some(String::from("port")),
            tls: None,
            receive_buffer_bytes: None,
            framing: None,
            decoding: default_decoding(),
            connection_limit: None,
        }
    }

    pub const fn host_key(&self) -> &Option<String> {
        &self.host_key
    }

    pub const fn tls(&self) -> &Option<TlsEnableableConfig> {
        &self.tls
    }

    pub const fn framing(&self) -> &Option<FramingConfig> {
        &self.framing
    }

    pub const fn decoding(&self) -> &DeserializerConfig {
        &self.decoding
    }

    pub const fn address(&self) -> SocketListenAddr {
        self.address
    }

    pub const fn keepalive(&self) -> Option<TcpKeepaliveConfig> {
        self.keepalive
    }

    pub const fn max_length(&self) -> Option<usize> {
        self.max_length
    }

    pub const fn shutdown_timeout_secs(&self) -> u64 {
        self.shutdown_timeout_secs
    }

    pub const fn receive_buffer_bytes(&self) -> Option<usize> {
        self.receive_buffer_bytes
    }

    pub fn set_max_length(&mut self, val: Option<usize>) -> &mut Self {
        self.max_length = val;
        self
    }

    pub fn set_shutdown_timeout_secs(&mut self, val: u64) -> &mut Self {
        self.shutdown_timeout_secs = val;
        self
    }

    pub fn set_tls(&mut self, val: Option<TlsEnableableConfig>) -> &mut Self {
        self.tls = val;
        self
    }

    pub fn set_framing(&mut self, val: Option<FramingConfig>) -> &mut Self {
        self.framing = val;
        self
    }

    pub fn set_decoding(&mut self, val: DeserializerConfig) -> &mut Self {
        self.decoding = val;
        self
    }
}

#[derive(Debug, Clone)]
pub struct RawTcpSource {
    config: TcpConfig,
    decoder: Decoder,
}

impl RawTcpSource {
    pub const fn new(config: TcpConfig, decoder: Decoder) -> Self {
        Self { config, decoder }
    }
}

impl TcpSource for RawTcpSource {
    type Error = codecs::decoding::Error;
    type Item = SmallVec<[Event; 1]>;
    type Decoder = Decoder;
    type Acker = TcpNullAcker;

    fn decoder(&self) -> Self::Decoder {
        self.decoder.clone()
    }

    fn handle_events(&self, events: &mut [Event], host: std::net::SocketAddr) {
        let now = Utc::now();

        for event in events {
            if let Event::Log(ref mut log) = event {
                log.try_insert(log_schema().source_type_key(), Bytes::from("socket"));
                log.try_insert(log_schema().timestamp_key(), now);

                let host_key = self
                    .config
                    .host_key
                    .as_deref()
                    .unwrap_or_else(|| log_schema().host_key());

                log.try_insert(host_key, host.ip().to_string());
                if let Some(port_key) = &self.config.port_key {
                    log.try_insert(port_key.as_str(), host.port());
                }
            }
        }
    }

    fn build_acker(&self, _: &[Self::Item]) -> Self::Acker {
        TcpNullAcker
    }
}
