use std::net::SocketAddr;

use futures::{FutureExt, StreamExt, TryFutureExt};
use serde::{Deserialize, Serialize};
use tokio::net::TcpStream;
use tonic::{
    transport::{server::Connected, Certificate, Server},
    Request, Response, Status,
};
use tracing::{Instrument, Span};
use vector_core::{
    event::{BatchNotifier, BatchStatus, BatchStatusReceiver, Event},
    ByteSizeOf,
};

use crate::{
    config::{AcknowledgementsConfig, DataType, GenerateConfig, Output, Resource, SourceContext},
    internal_events::{EventsReceived, StreamClosedError, TcpBytesReceived},
    proto::vector as proto,
    serde::bool_or_struct,
    shutdown::ShutdownSignalToken,
    sources::{util::AfterReadExt as _, Source},
    tls::{MaybeTlsIncomingStream, MaybeTlsSettings, TlsEnableableConfig},
    SourceSender,
};

#[derive(Debug, Clone)]
pub struct Service {
    pipeline: SourceSender,
    acknowledgements: bool,
}

#[tonic::async_trait]
impl proto::Service for Service {
    async fn push_events(
        &self,
        request: Request<proto::PushEventsRequest>,
    ) -> Result<Response<proto::PushEventsResponse>, Status> {
        let mut events: Vec<Event> = request
            .into_inner()
            .events
            .into_iter()
            .map(Event::from)
            .collect();

        let count = events.len();
        let byte_size = events.size_of();

        emit!(EventsReceived { count, byte_size });

        let receiver = BatchNotifier::maybe_apply_to_events(self.acknowledgements, &mut events);

        self.pipeline
            .clone()
            .send_batch(events)
            .map_err(|error| {
                let message = error.to_string();
                emit!(StreamClosedError { error, count });
                Status::unavailable(message)
            })
            .and_then(|_| handle_batch_status(receiver))
            .await?;

        Ok(Response::new(proto::PushEventsResponse {}))
    }

    // TODO: figure out a way to determine if the current Vector instance is "healthy".
    async fn health_check(
        &self,
        _: Request<proto::HealthCheckRequest>,
    ) -> Result<Response<proto::HealthCheckResponse>, Status> {
        let message = proto::HealthCheckResponse {
            status: proto::ServingStatus::Serving.into(),
        };

        Ok(Response::new(message))
    }
}

async fn handle_batch_status(receiver: Option<BatchStatusReceiver>) -> Result<(), Status> {
    let status = match receiver {
        Some(receiver) => receiver.await,
        None => BatchStatus::Delivered,
    };

    match status {
        BatchStatus::Errored => Err(Status::internal("Delivery error")),
        BatchStatus::Rejected => Err(Status::data_loss("Delivery failed")),
        BatchStatus::Delivered => Ok(()),
    }
}
#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct VectorConfig {
    pub address: SocketAddr,
    #[serde(default = "default_shutdown_timeout_secs")]
    pub shutdown_timeout_secs: u64,
    #[serde(default)]
    tls: Option<TlsEnableableConfig>,
    #[serde(default, deserialize_with = "bool_or_struct")]
    acknowledgements: AcknowledgementsConfig,
}

const fn default_shutdown_timeout_secs() -> u64 {
    30
}

impl GenerateConfig for VectorConfig {
    fn generate_config() -> toml::Value {
        toml::Value::try_from(Self {
            address: "0.0.0.0:6000".parse().unwrap(),
            shutdown_timeout_secs: default_shutdown_timeout_secs(),
            tls: None,
            acknowledgements: Default::default(),
        })
        .unwrap()
    }
}

impl VectorConfig {
    pub(super) async fn build(&self, cx: SourceContext) -> crate::Result<Source> {
        let tls_settings = MaybeTlsSettings::from_config(&self.tls, true)?;
        let acknowledgements = cx.do_acknowledgements(&self.acknowledgements);

        let source = run(self.address, tls_settings, cx, acknowledgements).map_err(|error| {
            error!(message = "Source future failed.", %error);
        });

        Ok(Box::pin(source))
    }

    pub(super) fn outputs(&self) -> Vec<Output> {
        vec![Output::default(DataType::all())]
    }

    pub(super) const fn source_type(&self) -> &'static str {
        "vector"
    }

    pub(super) fn resources(&self) -> Vec<Resource> {
        vec![Resource::tcp(self.address)]
    }
}

async fn run(
    address: SocketAddr,
    tls_settings: MaybeTlsSettings,
    cx: SourceContext,
    acknowledgements: bool,
) -> crate::Result<()> {
    let span = Span::current();

    let service = proto::Server::new(Service {
        pipeline: cx.out,
        acknowledgements,
    })
    .accept_gzip();

    let (tx, rx) = tokio::sync::oneshot::channel::<ShutdownSignalToken>();

    let listener = tls_settings.bind(&address).await?;
    let stream = listener.accept_stream().map(|result| {
        result.map(|socket| {
            let peer_addr = socket.connect_info().remote_addr;
            // TODO: Primary downside to this approach is that it's counting the raw bytes on the wire, which
            // will almost certainly be a much smaller number than the actual number of bytes when
            // accounting for compression.
            //
            // Possible solutions:
            // - write our own codec that works with `tonic::codec::Codec`, and write our own
            //   `prost_build::ServiceGenerator` that codegens using it, so that we can capture the size of the body
            //   after `tonic` decompresses it
            // - fork `tonic-build` to support overriding `Service::CODEC_PATH` in the builder config (see
            //   https://github.com/bmwill/tonic/commit/c409121811844494728a9ea8345ed2189855c870 for an example of doing
            //   this) and try and upstream it; we would still need to write the aforementioned codec, though
            // - switch to using the compression layer from `tower-http` to handle compression _before_ it hits `tonic`,
            //   which would then let us insert a layer right after it that would have access to the raw body before it
            //   gets decoded
            //
            // Number #3 is _probably_ easiest because it's "just" normal Tower middleware/layers, and there's already
            // the layer for handling compression, and it would give us more flexibility around only tracking the bytes
            // of specific service methods -- i.e. `push_events` -- rather than tracking all bytes that flow over the
            // gRPC connection, like health checks or any future enhancements that we/tonic adds.
            socket.after_read(move |byte_size| {
                emit!(TcpBytesReceived {
                    byte_size,
                    peer_addr,
                })
            })
        })
    });

    Server::builder()
        .trace_fn(move |_| span.clone())
        .add_service(service)
        .serve_with_incoming_shutdown(stream, cx.shutdown.map(|token| tx.send(token).unwrap()))
        .in_current_span()
        .await?;

    drop(rx.await);

    Ok(())
}

#[derive(Clone)]
pub struct MaybeTlsConnectInfo {
    pub remote_addr: SocketAddr,
    pub peer_certs: Option<Vec<Certificate>>,
}

impl Connected for MaybeTlsIncomingStream<TcpStream> {
    type ConnectInfo = MaybeTlsConnectInfo;

    fn connect_info(&self) -> Self::ConnectInfo {
        MaybeTlsConnectInfo {
            remote_addr: self.peer_addr(),
            peer_certs: self
                .ssl_stream()
                .and_then(|s| s.ssl().peer_cert_chain())
                .map(|s| {
                    s.into_iter()
                        .filter_map(|c| c.to_pem().ok())
                        .map(Certificate::from_pem)
                        .collect()
                }),
        }
    }
}

#[cfg(feature = "sinks-vector")]
#[cfg(test)]
mod tests {
    use vector_common::assert_event_data_eq;

    use super::*;
    use crate::{
        config::SinkContext,
        sinks::vector::v2::VectorConfig as SinkConfig,
        test_util::{
            self,
            components::{assert_source_compliance, SOCKET_PUSH_SOURCE_TAGS},
        },
        SourceSender,
    };

    #[tokio::test]
    async fn receive_message() {
        assert_source_compliance(&SOCKET_PUSH_SOURCE_TAGS, async {
            let addr = test_util::next_addr();
            let config = format!(r#"address = "{}""#, addr);
            let source: VectorConfig = toml::from_str(&config).unwrap();

            let (tx, rx) = SourceSender::new_test();
            let server = source
                .build(SourceContext::new_test(tx, None))
                .await
                .unwrap();
            tokio::spawn(server);
            test_util::wait_for_tcp(addr).await;

            // Ideally, this would be a fully custom agent to send the data,
            // but the sink side already does such a test and this is good
            // to ensure interoperability.
            let config = format!(r#"address = "{}""#, addr);
            let sink: SinkConfig = toml::from_str(&config).unwrap();
            let cx = SinkContext::new_test();
            let (sink, _) = sink.build(cx).await.unwrap();

            let (events, stream) = test_util::random_events_with_stream(100, 100, None);
            sink.run(stream).await.unwrap();

            let output = test_util::collect_ready(rx).await;
            assert_event_data_eq!(events, output);
        })
        .await;
    }

    #[tokio::test]
    async fn receive_compressed_message() {
        assert_source_compliance(&SOCKET_PUSH_SOURCE_TAGS, async {
            let addr = test_util::next_addr();
            let config = format!(r#"address = "{}""#, addr);
            let source: VectorConfig = toml::from_str(&config).unwrap();

            let (tx, rx) = SourceSender::new_test();
            let server = source
                .build(SourceContext::new_test(tx, None))
                .await
                .unwrap();
            tokio::spawn(server);
            test_util::wait_for_tcp(addr).await;

            // Ideally, this would be a fully custom agent to send the data,
            // but the sink side already does such a test and this is good
            // to ensure interoperability.
            let config = format!(
                r#"address = "{}"
            compression=true"#,
                addr
            );
            let sink: SinkConfig = toml::from_str(&config).unwrap();
            let cx = SinkContext::new_test();
            let (sink, _) = sink.build(cx).await.unwrap();

            let (events, stream) = test_util::random_events_with_stream(100, 100, None);
            sink.run(stream).await.unwrap();

            let output = test_util::collect_ready(rx).await;
            assert_event_data_eq!(events, output);
        })
        .await;
    }
}
