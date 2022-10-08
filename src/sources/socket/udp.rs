use std::net::SocketAddr;

use bytes::{Bytes, BytesMut};
use chrono::Utc;
use codecs::{
    decoding::{DeserializerConfig, FramingConfig},
    StreamDecodingError,
};
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use tokio::net::UdpSocket;
use tokio_util::codec::FramedRead;
use vector_core::ByteSizeOf;

use crate::{
    codecs::Decoder,
    config::log_schema,
    event::Event,
    internal_events::{
        BytesReceived, SocketEventsReceived, SocketMode, SocketReceiveError, StreamClosedError,
    },
    serde::{default_decoding, default_framing_message_based},
    shutdown::ShutdownSignal,
    sources::Source,
    udp, SourceSender,
};

/// UDP processes messages per packet, where messages are separated by newline.
#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct UdpConfig {
    address: SocketAddr,
    #[serde(default = "crate::serde::default_max_length")]
    pub(super) max_length: usize,
    host_key: Option<String>,
    port_key: Option<String>,
    receive_buffer_bytes: Option<usize>,
    #[serde(default = "default_framing_message_based")]
    pub(super) framing: FramingConfig,
    #[serde(default = "default_decoding")]
    decoding: DeserializerConfig,
}

impl UdpConfig {
    pub(super) const fn host_key(&self) -> &Option<String> {
        &self.host_key
    }

    pub(super) const fn framing(&self) -> &FramingConfig {
        &self.framing
    }

    pub(super) const fn decoding(&self) -> &DeserializerConfig {
        &self.decoding
    }

    pub(super) const fn address(&self) -> SocketAddr {
        self.address
    }

    pub fn from_address(address: SocketAddr) -> Self {
        Self {
            address,
            max_length: crate::serde::default_max_length(),
            host_key: None,
            port_key: Some(String::from("port")),
            receive_buffer_bytes: None,
            framing: default_framing_message_based(),
            decoding: default_decoding(),
        }
    }
}

pub(super) fn udp(
    config: UdpConfig,
    host_key: String,
    decoder: Decoder,
    mut shutdown: ShutdownSignal,
    mut out: SourceSender,
) -> Source {
    Box::pin(async move {
        let socket = UdpSocket::bind(&config.address)
            .await
            .expect("Failed to bind to udp listener socket");

        if let Some(receive_buffer_bytes) = config.receive_buffer_bytes {
            if let Err(error) = udp::set_receive_buffer_size(&socket, receive_buffer_bytes) {
                warn!(message = "Failed configuring receive buffer size on UDP socket.", %error);
            }
        }

        let max_length = match config.receive_buffer_bytes {
            Some(receive_buffer_bytes) => std::cmp::min(config.max_length, receive_buffer_bytes),
            None => config.max_length,
        };

        info!(message = "Listening.", address = %config.address);

        // We add 1 to the max_length in order to determine if the received data has been truncated.
        let mut buf = BytesMut::with_capacity(max_length + 1);
        loop {
            buf.resize(max_length + 1, 0);
            tokio::select! {
                recv = socket.recv_from(&mut buf) => {
                    let (byte_size, address) = match recv {
                        Ok(res) => res,
                        Err(error) => {
                            #[cfg(windows)]
                            if let Some(err) = error.raw_os_error() {
                                if err == 10040 {
                                    // 10040 is the Windows error that the Udp message has exceeded max_length
                                    warn!(
                                        message = "Discarding frame larger than max_length.",
                                        max_length = max_length,
                                        internal_log_rate_secs = 30
                                    );
                                    continue;
                                }
                            }

                            let error = codecs::decoding::Error::FramingError(error.into());
                            return Err(emit!(SocketReceiveError {
                                mode: SocketMode::Udp,
                                error: &error
                            }));
                       }
                    };

                    emit!(BytesReceived { byte_size, protocol: "udp" });

                    let payload = buf.split_to(byte_size);
                    let truncated = byte_size == max_length + 1;

                    let mut stream = FramedRead::new(payload.as_ref(), decoder.clone()).peekable();

                    while let Some(result) = stream.next().await {
                        let last = Pin::new(&mut stream).peek().await.is_none();
                        match result {
                            Ok((mut events, _byte_size)) => {
                                if last && truncated {
                                    // The last event in this payload was truncated, so we want to drop it.
                                    let _ = events.pop();
                                    warn!(
                                        message = "Discarding frame larger than max_length.",
                                        max_length = max_length,
                                        internal_log_rate_secs = 30
                                    );
                                }

                                if events.is_empty() {
                                    continue;
                                }

                                let count = events.len();
                                emit!(SocketEventsReceived {
                                    mode: SocketMode::Udp,
                                    byte_size: events.size_of(),
                                    count,
                                });

                                let now = Utc::now();

                                for event in &mut events {
                                    if let Event::Log(ref mut log) = event {
                                        log.try_insert(log_schema().source_type_key(), Bytes::from("socket"));
                                        log.try_insert(log_schema().timestamp_key(), now);
                                        log.try_insert(host_key.as_str(), address.ip().to_string());

                                        if let Some(port_key) = &config.port_key {
                                            log.try_insert(port_key.as_str(), address.port());
                                        }
                                    }
                                }

                                tokio::select!{
                                    result = out.send_batch(events) => {
                                        if let Err(error) = result {
                                            emit!(StreamClosedError { error, count });
                                            return Ok(())
                                        }
                                    }
                                    _ = &mut shutdown => return Ok(()),
                                }
                            }
                            Err(error) => {
                                // Error is logged by `crate::codecs::Decoder`, no
                                // further handling is needed here.
                                if !error.can_continue() {
                                    break;
                                }
                            }
                        }
                    }
                }
                _ = &mut shutdown => return Ok(()),
            }
        }
    })
}
