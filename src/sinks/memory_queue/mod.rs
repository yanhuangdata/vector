mod config;
mod sink;

pub use config::MemoryQueueConfig;
pub use sink::MemoryQueueSink;

#[cfg(test)]
mod tests {
    use crate::test_util::random_events_with_stream;
    // use vector_core::event::EventContainer;

    use crate::{
        sinks::{
            memory_queue::{config::MemoryQueueConfig, sink::MemoryQueueSink},
            VectorSink,
        },
    };

    #[test]
    fn generate_config() {
        crate::test_util::test_generate_config::<MemoryQueueConfig>();
    }

    #[tokio::test]
    async fn memory_queue() {
        let config = MemoryQueueConfig {
            rate: None,
            acknowledgements: Default::default(),
            queue_size: None
        };
        let sink = MemoryQueueSink::new(config);
        let message_receiver = MemoryQueueSink::take_message_receiver();
        assert!(message_receiver.is_some());

        let stream_sink = VectorSink::Stream(Box::new(sink));

        let (_input_lines, events) = random_events_with_stream(100, 10, None);
        let _ = stream_sink.run(Box::pin(events)).await.unwrap();
    }
}
