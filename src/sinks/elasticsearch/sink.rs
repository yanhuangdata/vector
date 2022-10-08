use std::{fmt, num::NonZeroUsize};

use async_trait::async_trait;
use futures::{future, stream::BoxStream, StreamExt};
use tower::Service;
use vector_core::{
    buffers::Acker,
    stream::{BatcherSettings, DriverResponse},
    ByteSizeOf,
};

use crate::{
    event::{Event, LogEvent, Value},
    sinks::{
        elasticsearch::{
            encoder::ProcessedEvent, request_builder::ElasticsearchRequestBuilder,
            service::ElasticsearchRequest, BulkAction, ElasticsearchCommonMode,
        },
        util::{Compression, SinkBuilderExt, StreamSink},
    },
    transforms::metric_to_log::MetricToLog,
};

#[derive(Clone, Eq, Hash, PartialEq)]
pub struct PartitionKey {
    pub index: String,
    pub bulk_action: BulkAction,
}

pub struct BatchedEvents {
    pub key: PartitionKey,
    pub events: Vec<ProcessedEvent>,
}

impl ByteSizeOf for BatchedEvents {
    fn allocated_bytes(&self) -> usize {
        self.events.size_of()
    }
}

pub struct ElasticsearchSink<S> {
    pub batch_settings: BatcherSettings,
    pub request_builder: ElasticsearchRequestBuilder,
    pub compression: Compression,
    pub service: S,
    pub acker: Acker,
    pub metric_to_log: MetricToLog,
    pub mode: ElasticsearchCommonMode,
    pub id_key_field: Option<String>,
}

impl<S> ElasticsearchSink<S>
where
    S: Service<ElasticsearchRequest> + Send + 'static,
    S::Future: Send + 'static,
    S::Response: DriverResponse + Send + 'static,
    S::Error: fmt::Debug + Into<crate::Error> + Send,
{
    pub async fn run_inner(self: Box<Self>, input: BoxStream<'_, Event>) -> Result<(), ()> {
        let request_builder_concurrency_limit = NonZeroUsize::new(50);

        let mode = self.mode;
        let id_key_field = self.id_key_field;

        let sink = input
            .scan(self.metric_to_log, |metric_to_log, event| {
                future::ready(Some(match event {
                    Event::Metric(metric) => metric_to_log.transform_one(metric),
                    Event::Log(log) => Some(log),
                    _ => None,
                }))
            })
            .filter_map(|x| async move { x })
            .filter_map(move |log| future::ready(process_log(log, &mode, &id_key_field)))
            .batched(self.batch_settings.into_byte_size_config())
            .request_builder(request_builder_concurrency_limit, self.request_builder)
            .filter_map(|request| async move {
                match request {
                    Err(e) => {
                        error!("Failed to build Elasticsearch request: {:?}.", e);
                        None
                    }
                    Ok(req) => Some(req),
                }
            })
            .into_driver(self.service, self.acker);

        sink.run().await
    }
}

pub fn process_log(
    mut log: LogEvent,
    mode: &ElasticsearchCommonMode,
    id_key_field: &Option<String>,
) -> Option<ProcessedEvent> {
    let index = mode.index(&log)?;
    let bulk_action = mode.bulk_action(&log)?;

    if let Some(cfg) = mode.as_data_stream_config() {
        cfg.sync_fields(&mut log);
        cfg.remap_timestamp(&mut log);
    };
    let id = if let Some(Value::Bytes(key)) = id_key_field
        .as_ref()
        .and_then(|key| log.remove(key.as_str()))
    {
        Some(String::from_utf8_lossy(&key).into_owned())
    } else {
        None
    };
    Some(ProcessedEvent {
        index,
        bulk_action,
        log,
        id,
    })
}

#[async_trait]
impl<S> StreamSink<Event> for ElasticsearchSink<S>
where
    S: Service<ElasticsearchRequest> + Send + 'static,
    S::Future: Send + 'static,
    S::Response: DriverResponse + Send + 'static,
    S::Error: fmt::Debug + Into<crate::Error> + Send,
{
    async fn run(self: Box<Self>, input: BoxStream<'_, Event>) -> Result<(), ()> {
        self.run_inner(input).await
    }
}
