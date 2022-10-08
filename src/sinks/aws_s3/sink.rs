use std::io;

use bytes::Bytes;
use chrono::Utc;
use codecs::encoding::Framer;
use uuid::Uuid;
use vector_core::{event::Finalizable, ByteSizeOf};

use crate::{
    codecs::Encoder,
    event::Event,
    sinks::{
        s3_common::{
            config::S3Options,
            service::{S3Metadata, S3Request},
        },
        util::{encoding::Transformer, request_builder::EncodeResult, Compression, RequestBuilder},
    },
};

#[derive(Clone)]
pub struct S3RequestOptions {
    pub bucket: String,
    pub filename_time_format: String,
    pub filename_append_uuid: bool,
    pub filename_extension: Option<String>,
    pub api_options: S3Options,
    pub encoder: (Transformer, Encoder<Framer>),
    pub compression: Compression,
}

impl RequestBuilder<(String, Vec<Event>)> for S3RequestOptions {
    type Metadata = S3Metadata;
    type Events = Vec<Event>;
    type Encoder = (Transformer, Encoder<Framer>);
    type Payload = Bytes;
    type Request = S3Request;
    type Error = io::Error; // TODO: this is ugly.

    fn compression(&self) -> Compression {
        self.compression
    }

    fn encoder(&self) -> &Self::Encoder {
        &self.encoder
    }

    fn split_input(&self, input: (String, Vec<Event>)) -> (Self::Metadata, Self::Events) {
        let (partition_key, mut events) = input;
        let finalizers = events.take_finalizers();
        let metadata = S3Metadata {
            partition_key,
            count: events.len(),
            byte_size: events.size_of(),
            finalizers,
        };

        (metadata, events)
    }

    fn build_request(
        &self,
        mut metadata: Self::Metadata,
        payload: EncodeResult<Self::Payload>,
    ) -> Self::Request {
        let filename = {
            let formatted_ts = Utc::now().format(self.filename_time_format.as_str());

            self.filename_append_uuid
                .then(|| format!("{}-{}", formatted_ts, Uuid::new_v4().hyphenated()))
                .unwrap_or_else(|| formatted_ts.to_string())
        };

        let extension = self
            .filename_extension
            .as_ref()
            .cloned()
            .unwrap_or_else(|| self.compression.extension().into());
        metadata.partition_key = format!("{}{}.{}", metadata.partition_key, filename, extension);

        S3Request {
            body: payload.into_payload(),
            bucket: self.bucket.clone(),
            metadata,
            content_encoding: self.compression.content_encoding(),
            options: self.api_options.clone(),
        }
    }
}
