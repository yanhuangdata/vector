use std::io;

use aws_sdk_kinesis::model::PutRecordsRequestEntry;
use aws_sdk_kinesis::types::Blob;
use bytes::Bytes;
use vector_core::{buffers::Ackable, ByteSizeOf};

use crate::{
    codecs::Encoder,
    event::{Event, EventFinalizers, Finalizable},
    sinks::{
        aws_kinesis_streams::sink::KinesisProcessedEvent,
        util::{encoding::Transformer, request_builder::EncodeResult, Compression, RequestBuilder},
    },
};

pub struct KinesisRequestBuilder {
    pub compression: Compression,
    pub encoder: (Transformer, Encoder<()>),
}

pub struct Metadata {
    pub finalizers: EventFinalizers,
    pub partition_key: String,
    pub event_byte_size: usize,
}

#[derive(Clone)]
pub struct KinesisRequest {
    pub put_records_request: PutRecordsRequestEntry,
    pub finalizers: EventFinalizers,
    pub event_byte_size: usize,
}

impl Ackable for KinesisRequest {
    fn ack_size(&self) -> usize {
        1
    }
}

impl Finalizable for KinesisRequest {
    fn take_finalizers(&mut self) -> EventFinalizers {
        std::mem::take(&mut self.finalizers)
    }
}

impl KinesisRequest {
    fn encoded_length(&self) -> usize {
        let hash_key_size = self
            .put_records_request
            .explicit_hash_key
            .as_ref()
            .map(|s| s.len())
            .unwrap_or_default();

        // data is base64 encoded
        let data_len = self
            .put_records_request
            .data
            .as_ref()
            .map(|data| data.as_ref().len())
            .unwrap_or(0);

        let key_len = self
            .put_records_request
            .partition_key
            .as_ref()
            .map(|key| key.len())
            .unwrap_or(0);

        (data_len + 2) / 3 * 4 + hash_key_size + key_len + 10
    }
}

impl ByteSizeOf for KinesisRequest {
    fn size_of(&self) -> usize {
        // `ByteSizeOf` is being somewhat abused here. This is
        // used by the batcher. `encoded_length` is needed so that final
        // batched size doesn't exceed the Kinesis limits (5Mb)
        self.encoded_length()
    }

    fn allocated_bytes(&self) -> usize {
        0
    }
}

impl RequestBuilder<KinesisProcessedEvent> for KinesisRequestBuilder {
    type Metadata = Metadata;
    type Events = Event;
    type Encoder = (Transformer, Encoder<()>);
    type Payload = Bytes;
    type Request = KinesisRequest;
    type Error = io::Error;

    fn compression(&self) -> Compression {
        self.compression
    }

    fn encoder(&self) -> &Self::Encoder {
        &self.encoder
    }

    fn split_input(&self, mut event: KinesisProcessedEvent) -> (Self::Metadata, Self::Events) {
        let metadata = Metadata {
            finalizers: event.event.take_finalizers(),
            partition_key: event.metadata.partition_key,
            event_byte_size: event.event.size_of(),
        };
        (metadata, Event::from(event.event))
    }

    fn build_request(
        &self,
        metadata: Self::Metadata,
        payload: EncodeResult<Self::Payload>,
    ) -> Self::Request {
        let payload = payload.into_payload();
        KinesisRequest {
            put_records_request: PutRecordsRequestEntry::builder()
                .data(Blob::new(&payload[..]))
                .partition_key(metadata.partition_key)
                .build(),
            finalizers: metadata.finalizers,
            event_byte_size: metadata.event_byte_size,
        }
    }
}
