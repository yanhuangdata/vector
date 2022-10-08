use std::{
    fmt::Debug,
    sync::Arc,
    task::{Context, Poll},
};

use bytes::Bytes;
use futures::future::BoxFuture;
use http::{
    header::{CONTENT_ENCODING, CONTENT_LENGTH, CONTENT_TYPE},
    Request,
};
use hyper::Body;
use tower::Service;
use tracing::Instrument;
use vector_common::internal_event::BytesSent;
use vector_core::{
    buffers::Ackable,
    event::{EventFinalizers, EventStatus, Finalizable},
    internal_event::EventsSent,
    stream::DriverResponse,
};

use super::{NewRelicCredentials, NewRelicSinkError};
use crate::{
    http::{get_http_scheme_from_uri, HttpClient},
    sinks::util::{metadata::RequestMetadata, Compression},
};

#[derive(Debug, Clone)]
pub struct NewRelicApiRequest {
    pub metadata: RequestMetadata,
    pub finalizers: EventFinalizers,
    pub credentials: Arc<NewRelicCredentials>,
    pub payload: Bytes,
    pub compression: Compression,
}

impl Ackable for NewRelicApiRequest {
    fn ack_size(&self) -> usize {
        self.metadata.event_count()
    }
}

impl Finalizable for NewRelicApiRequest {
    fn take_finalizers(&mut self) -> EventFinalizers {
        std::mem::take(&mut self.finalizers)
    }
}

#[derive(Debug)]
pub struct NewRelicApiResponse {
    event_status: EventStatus,
    protocol: &'static str,
    metadata: RequestMetadata,
}

impl DriverResponse for NewRelicApiResponse {
    fn event_status(&self) -> EventStatus {
        self.event_status
    }

    fn events_sent(&self) -> EventsSent {
        EventsSent {
            count: self.metadata.event_count(),
            byte_size: self.metadata.events_byte_size(),
            output: None,
        }
    }

    fn bytes_sent(&self) -> Option<BytesSent> {
        Some(BytesSent {
            byte_size: self.metadata.request_encoded_size(),
            protocol: self.protocol,
        })
    }
}

#[derive(Debug, Clone)]
pub struct NewRelicApiService {
    pub client: HttpClient,
}

impl Service<NewRelicApiRequest> for NewRelicApiService {
    type Response = NewRelicApiResponse;
    type Error = NewRelicSinkError;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: NewRelicApiRequest) -> Self::Future {
        let mut client = self.client.clone();

        let uri = request.credentials.get_uri();
        let protocol = get_http_scheme_from_uri(&uri);

        let http_request = Request::post(&uri)
            .header(CONTENT_TYPE, "application/json")
            .header("Api-Key", request.credentials.license_key.clone());

        let http_request = if let Some(ce) = request.compression.content_encoding() {
            http_request.header(CONTENT_ENCODING, ce)
        } else {
            http_request
        };

        let payload_len = request.payload.len();
        let metadata = request.metadata;
        let http_request = http_request
            .header(CONTENT_LENGTH, payload_len)
            .body(Body::from(request.payload))
            .expect("building HTTP request failed unexpectedly");

        Box::pin(async move {
            match client.call(http_request).in_current_span().await {
                Ok(_) => Ok(NewRelicApiResponse {
                    event_status: EventStatus::Delivered,
                    metadata,
                    protocol,
                }),
                Err(_) => Err(NewRelicSinkError::new("HTTP request error")),
            }
        })
    }
}
