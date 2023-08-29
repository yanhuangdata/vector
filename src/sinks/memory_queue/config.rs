use futures::{future, FutureExt};
use vector_config::configurable_component;


use crate::{
    config::{AcknowledgementsConfig, GenerateConfig, Input, SinkConfig, SinkContext, SinkDescription},
    sinks::{memory_queue::sink::MemoryQueueSink, Healthcheck, VectorSink},
};

/// Configuration for the `memory_queue` sink.
#[configurable_component(sink("memory_queue", "Send events to built-in memory queue"))]
#[derive(Clone, Debug, Derivative)]
#[serde(deny_unknown_fields, default)]
#[derivative(Default)]
pub struct MemoryQueueConfig {
    /// The number of events, per second, that the sink is allowed to consume.
    ///
    /// By default, there is no limit.
    pub rate: Option<usize>,

    #[configurable(derived)]
    #[serde(
        default,
        deserialize_with = "crate::serde::bool_or_struct",
        skip_serializing_if = "crate::serde::skip_serializing_if_default"
    )]
    pub acknowledgements: AcknowledgementsConfig,

    /// the size of the message queue
    pub queue_size: Option<usize>,
}

inventory::submit! {
    SinkDescription::new::<MemoryQueueConfig>("memory_queue", "sink", "memory_queue_sink", "a builtin memory queue sink")
}

impl GenerateConfig for MemoryQueueConfig {
    fn generate_config() -> toml::Value {
        toml::Value::try_from(&Self::default()).unwrap()
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "memory_queue")]
impl SinkConfig for MemoryQueueConfig {
    async fn build(&self, _cx: SinkContext) -> crate::Result<(VectorSink, Healthcheck)> {

        let sink = MemoryQueueSink::new(self.clone());
        let healthcheck = future::ok(()).boxed();

        Ok((VectorSink::Stream(Box::new(sink)), healthcheck))
    }

    fn input(&self) -> Input {
        Input::all()
    }

    fn acknowledgements(&self) -> &AcknowledgementsConfig {
        &self.acknowledgements
    }
}
