mod config;
mod sink;

pub use config::MemoryQueueConfig;

use crate::config::SinkDescription;

inventory::submit! {
    SinkDescription::new::<MemoryQueueConfig>("memory_queue")
}

#[cfg(test)]
mod tests {

    use vector_buffers::Acker;

    use crate::{
        sinks::{
            memory_queue::{config::MemoryQueueConfig, sink::MemoryQueueSink},
            VectorSink,
        },
        test_util::{
            components::run_and_assert_nonsending_sink_compliance, random_events_with_stream,
        },
    };

    #[tokio::test]
    async fn memory_queue() {
        let config = MemoryQueueConfig {
            print_interval_secs: 10,
            rate: None,
            acknowledgements: Default::default(),
        };
        let sink = MemoryQueueSink::new(config, Acker::passthrough());
        let sink = VectorSink::Stream(Box::new(sink));

        let (_input_lines, events) = random_events_with_stream(100, 10, None);
        run_and_assert_nonsending_sink_compliance(sink, events, &[]).await;
    }
}
