use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use tokio::time::Duration;
use vector_buffers::{BufferConfig, BufferType, WhenFull};
use vector_core::config::MEMORY_BUFFER_DEFAULT_MAX_EVENTS;

use crate::config::SinkOuter;
use crate::topology::builder::SOURCE_SENDER_BUFFER_SIZE;
use crate::{config::Config, test_util::start_topology};

// Based on how we pump events from `SourceSender` into `Fanout`, there's always one extra event we
// may pull out of `SourceSender` but can't yet send into `Fanout`, so we account for that here.
pub(self) const EXTRA_SOURCE_PUMP_EVENT: usize = 1;

/// Connects a single source to a single sink and makes sure the sink backpressure is propagated
/// to the source.
#[tokio::test]
async fn serial_backpressure() {
    let mut config = Config::builder();

    let events_to_sink = 100;

    let expected_sourced_events = events_to_sink
        + MEMORY_BUFFER_DEFAULT_MAX_EVENTS.get()
        + *SOURCE_SENDER_BUFFER_SIZE
        + EXTRA_SOURCE_PUMP_EVENT;

    let source_counter = Arc::new(AtomicUsize::new(0));
    config.add_source(
        "in",
        test_source::TestBackpressureSourceConfig {
            counter: Arc::clone(&source_counter),
        },
    );
    config.add_sink(
        "out",
        &["in"],
        test_sink::TestBackpressureSinkConfig {
            num_to_consume: events_to_sink,
        },
    );

    let (_topology, _crash) = start_topology(config.build().unwrap(), false).await;

    // allow the topology to run
    wait_until_expected(&source_counter, expected_sourced_events).await;

    let sourced_events = source_counter.load(Ordering::Acquire);

    assert_eq!(sourced_events, expected_sourced_events);
}

/// Connects a single source to two sinks and makes sure that the source is only able
/// to emit events that the slower sink accepts.
#[tokio::test]
async fn default_fan_out() {
    let mut config = Config::builder();

    let events_to_sink = 100;

    let expected_sourced_events = events_to_sink
        + MEMORY_BUFFER_DEFAULT_MAX_EVENTS.get()
        + *SOURCE_SENDER_BUFFER_SIZE
        + EXTRA_SOURCE_PUMP_EVENT;

    let source_counter = Arc::new(AtomicUsize::new(0));
    config.add_source(
        "in",
        test_source::TestBackpressureSourceConfig {
            counter: Arc::clone(&source_counter),
        },
    );
    config.add_sink(
        "out1",
        &["in"],
        test_sink::TestBackpressureSinkConfig {
            num_to_consume: events_to_sink * 2,
        },
    );

    config.add_sink(
        "out2",
        &["in"],
        test_sink::TestBackpressureSinkConfig {
            num_to_consume: events_to_sink,
        },
    );

    let (_topology, _crash) = start_topology(config.build().unwrap(), false).await;

    // allow the topology to run
    wait_until_expected(&source_counter, expected_sourced_events).await;

    let sourced_events = source_counter.load(Ordering::Relaxed);

    assert_eq!(sourced_events, expected_sourced_events);
}

/// Connects a single source to two sinks. One of the sinks is configured to drop events that exceed
/// the buffer size. Asserts that the sink that drops events does not cause backpressure, but the
/// other one does.
#[tokio::test]
async fn buffer_drop_fan_out() {
    let mut config = Config::builder();

    let events_to_sink = 100;

    let expected_sourced_events = events_to_sink
        + MEMORY_BUFFER_DEFAULT_MAX_EVENTS.get()
        + *SOURCE_SENDER_BUFFER_SIZE
        + EXTRA_SOURCE_PUMP_EVENT;

    let source_counter = Arc::new(AtomicUsize::new(0));
    config.add_source(
        "in",
        test_source::TestBackpressureSourceConfig {
            counter: Arc::clone(&source_counter),
        },
    );
    config.add_sink(
        "out1",
        &["in"],
        test_sink::TestBackpressureSinkConfig {
            num_to_consume: events_to_sink,
        },
    );

    let mut sink_outer = SinkOuter::new(
        vec!["in".to_string()],
        Box::new(test_sink::TestBackpressureSinkConfig {
            num_to_consume: events_to_sink / 2,
        }),
    );
    sink_outer.buffer = BufferConfig {
        stages: vec![BufferType::Memory {
            max_events: MEMORY_BUFFER_DEFAULT_MAX_EVENTS,
            when_full: WhenFull::DropNewest,
        }],
    };
    config.add_sink_outer("out2", sink_outer);

    let (_topology, _crash) = start_topology(config.build().unwrap(), false).await;

    // allow the topology to run
    wait_until_expected(&source_counter, expected_sourced_events).await;

    let sourced_events = source_counter.load(Ordering::Relaxed);

    assert_eq!(sourced_events, expected_sourced_events);
}

/// Connects 2 sources to a single sink, and asserts that the sum of the events produced
/// by the sources is how many the single sink accepted.
#[tokio::test]
async fn multiple_inputs_backpressure() {
    let mut config = Config::builder();

    let events_to_sink = 100;

    let expected_sourced_events = events_to_sink
        + MEMORY_BUFFER_DEFAULT_MAX_EVENTS.get()
        + *SOURCE_SENDER_BUFFER_SIZE * 2
        + EXTRA_SOURCE_PUMP_EVENT * 2;

    let source_counter = Arc::new(AtomicUsize::new(0));
    config.add_source(
        "in1",
        test_source::TestBackpressureSourceConfig {
            counter: Arc::clone(&source_counter),
        },
    );
    config.add_source(
        "in2",
        test_source::TestBackpressureSourceConfig {
            counter: Arc::clone(&source_counter),
        },
    );
    config.add_sink(
        "out",
        &["in1", "in2"],
        test_sink::TestBackpressureSinkConfig {
            num_to_consume: events_to_sink,
        },
    );

    let (_topology, _crash) = start_topology(config.build().unwrap(), false).await;

    // allow the topology to run
    wait_until_expected(&source_counter, expected_sourced_events).await;

    let sourced_events_sum = source_counter.load(Ordering::Relaxed);

    assert_eq!(sourced_events_sum, expected_sourced_events);
}

// Wait until the source has sent at least the expected number of events, plus a small additional
// margin to ensure we allow it to run over the expected amount if it's going to.
async fn wait_until_expected(source_counter: impl AsRef<AtomicUsize>, expected: usize) {
    crate::test_util::wait_for_atomic_usize(source_counter, |count| count >= expected).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
}

mod test_sink {
    use async_trait::async_trait;
    use futures::stream::BoxStream;
    use futures::{FutureExt, StreamExt};
    use serde::{Deserialize, Serialize};

    use crate::config::{AcknowledgementsConfig, Input, SinkConfig, SinkContext};
    use crate::event::Event;
    use crate::sinks::util::StreamSink;
    use crate::sinks::{Healthcheck, VectorSink};

    #[derive(Debug)]
    struct TestBackpressureSink {
        // It consumes this many then stops.
        num_to_consume: usize,
    }

    #[async_trait]
    impl StreamSink<Event> for TestBackpressureSink {
        async fn run(self: Box<Self>, input: BoxStream<'_, Event>) -> Result<(), ()> {
            let _num_taken = input.take(self.num_to_consume).count().await;
            futures::future::pending::<()>().await;
            Ok(())
        }
    }

    #[derive(Debug, Serialize, Deserialize)]
    pub(super) struct TestBackpressureSinkConfig {
        pub num_to_consume: usize,
    }

    #[async_trait]
    #[typetag::serde(name = "test-backpressure-sink")]
    impl SinkConfig for TestBackpressureSinkConfig {
        async fn build(&self, _cx: SinkContext) -> crate::Result<(VectorSink, Healthcheck)> {
            let sink = TestBackpressureSink {
                num_to_consume: self.num_to_consume,
            };
            let healthcheck = futures::future::ok(()).boxed();
            Ok((VectorSink::from_event_streamsink(sink), healthcheck))
        }

        fn input(&self) -> Input {
            Input::all()
        }

        fn sink_type(&self) -> &'static str {
            "test-backpressure-sink"
        }

        fn acknowledgements(&self) -> Option<&AcknowledgementsConfig> {
            None
        }
    }
}

mod test_source {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use async_trait::async_trait;
    use futures::FutureExt;
    use serde::{Deserialize, Serialize};

    use crate::config::{DataType, Output, SourceConfig, SourceContext};
    use crate::event::Event;
    use crate::sources::Source;

    #[derive(Debug, Serialize, Deserialize)]
    pub struct TestBackpressureSourceConfig {
        // The number of events that have been sent.
        #[serde(skip)]
        pub counter: Arc<AtomicUsize>,
    }

    #[async_trait]
    #[typetag::serde(name = "test-backpressure-source")]
    impl SourceConfig for TestBackpressureSourceConfig {
        async fn build(&self, mut cx: SourceContext) -> crate::Result<Source> {
            let counter = Arc::clone(&self.counter);
            Ok(async move {
                for i in 0.. {
                    let _result = cx.out.send_event(Event::from(format!("event-{}", i))).await;
                    counter.fetch_add(1, Ordering::AcqRel);
                    // Place ourselves at the back of tokio's task queue, giving downstream
                    // components a chance to process the event we just sent before sending more.
                    // This helps the backpressure tests behave more deterministically when we use
                    // opportunistic batching at the topology level. Yielding here makes it very
                    // unlikely that a `ready_chunks` or similar will have a chance to see more
                    // than one event available at a time.
                    tokio::task::yield_now().await;
                }
                Ok(())
            }
            .boxed())
        }

        fn outputs(&self) -> Vec<Output> {
            vec![Output::default(DataType::all())]
        }

        fn source_type(&self) -> &'static str {
            "test-backpressure-source"
        }

        fn can_acknowledge(&self) -> bool {
            false
        }
    }
}
