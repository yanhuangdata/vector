use crate::{
    config::{DataType, GenerateConfig, SinkConfig, SinkContext, SinkDescription},
    sinks::util::StreamSink,
    topology::{GLOBAL_VEC_TX},
};
use async_trait::async_trait;
use futures::{future, stream::BoxStream, FutureExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::{
    sync::{
        atomic::{AtomicUsize},
        Arc,
    },
    time::{Duration, Instant},
};
use tokio::{
    sync::watch,
    time::sleep_until,
};
use vector_core::ByteSizeOf;
use vector_core::{buffers::Acker, event::Event};

pub struct MemoryQueueSink {
    total_events: Arc<AtomicUsize>,
    total_raw_bytes: Arc<AtomicUsize>,
    config: MemoryQueueConfig,
    acker: Acker,
    last: Option<Instant>,
}

#[derive(Clone, Debug, Derivative, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
#[derivative(Default)]
pub struct MemoryQueueConfig {
    pub rate: Option<usize>,
}

inventory::submit! {
    SinkDescription::new::<MemoryQueueConfig>("memory_queue")
}

impl GenerateConfig for MemoryQueueConfig {
    fn generate_config() -> toml::Value {
        toml::Value::try_from(&Self::default()).unwrap()
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "memory_queue")]
impl SinkConfig for MemoryQueueConfig {
    async fn build(
        &self,
        cx: SinkContext,
    ) -> crate::Result<(super::VectorSink, super::Healthcheck)> {
        let sink = MemoryQueueSink::new(self.clone(), cx.acker());
        let healthcheck = future::ok(()).boxed();

        Ok((super::VectorSink::Stream(Box::new(sink)), healthcheck))
    }

    fn input_type(&self) -> DataType {
        DataType::Any
    }

    fn sink_type(&self) -> &'static str {
        "memory_queue"
    }
}

impl MemoryQueueSink {
    pub fn new(config: MemoryQueueConfig, acker: Acker) -> Self {
        MemoryQueueSink {
            config,
            total_events: Arc::new(AtomicUsize::new(0)),
            total_raw_bytes: Arc::new(AtomicUsize::new(0)),
            acker,
            last: None,
        }
    }
}

#[async_trait]
impl StreamSink for MemoryQueueSink {
    async fn run(mut self: Box<Self>, input: BoxStream<'_, Event>) -> Result<(), ()> {
        let total_events = Arc::clone(&self.total_events);
        let total_raw_bytes = Arc::clone(&self.total_raw_bytes);
        let (shutdown, mut tripwire) = watch::channel(());

        let mut chunks = input.ready_chunks(1024);
        // let sender = unsafe { &GLOBAL_TX.unwrap() };
        while let Some(events) = chunks.next().await {
            if let Some(rate) = self.config.rate {
                let factor: f32 = 1.0 / rate as f32;
                let secs: f32 = factor * (events.len() as f32);
                let until = self.last.unwrap_or_else(Instant::now) + Duration::from_secs_f32(secs);
                sleep_until(until.into()).await;
                self.last = Some(until);
            }

            unsafe {
                if let Some(sender) = &mut GLOBAL_VEC_TX {
                    // sender.try_send(events.clone());
                    // we use a MPSC channel here, and set the buffer size to a limited count, which make sender
                    // only outpaces receiver by the limited count. When sender is full, we sleep and retry, and
                    // do not ack the source, to make source buffered on disk if source producing is too fast.
                    while let Err(send_err) = sender.try_send(events.clone()) {
                        if send_err.is_full() {
                            // queue is full, waiting
                            let sleep_time = std::time::Duration::from_millis(100);
                            std::thread::sleep(sleep_time);
                        } else if send_err.is_disconnected() {
                            error!("failed to send events to memory queue due to disconnected error");
                        } else {
                            break;
                        }
                    }
                }
            }

            let message_len = events.size_of();

            // let _ = self.total_events.fetch_add(events.len(), Ordering::AcqRel);
            // let _ = self
            //     .total_raw_bytes
            //     .fetch_add(message_len, Ordering::AcqRel);

            // emit!(&BlackholeEventReceived {
            //     byte_size: message_len
            // });

            // BRIAN TODO: may do some refine on acker logic here
            self.acker.ack(events.len());
        }

        // Notify the reporting task to shutdown.
        let _ = shutdown.send(());

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_util::random_events_with_stream;

    #[test]
    fn generate_config() {
        crate::test_util::test_generate_config::<MemoryQueueConfig>();
    }

    #[tokio::test]
    async fn memory_queue() {
        let config = MemoryQueueConfig {
            rate: None,
            // out_sender: None,
        };
        let sink = Box::new(MemoryQueueSink::new(config, Acker::Null));

        let (_input_lines, events) = random_events_with_stream(100, 10, None);
        let _ = sink.run(Box::pin(events)).await.unwrap();
    }
}

#[cfg(test)]
mod tests_memory_queue_sink {
    #[test]
    fn it_works() {
        println!("memory_queue test placeholder");
    }
}
