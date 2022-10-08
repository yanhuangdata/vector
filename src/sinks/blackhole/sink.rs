use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use async_trait::async_trait;
use futures::{stream::BoxStream, StreamExt};
use tokio::{
    select,
    sync::watch,
    time::{interval, sleep_until},
};
use vector_core::{buffers::Acker, internal_event::EventsSent, ByteSizeOf};

use crate::{
    event::{EventArray, EventContainer},
    sinks::{blackhole::config::BlackholeConfig, util::StreamSink},
};

pub struct BlackholeSink {
    total_events: Arc<AtomicUsize>,
    total_raw_bytes: Arc<AtomicUsize>,
    config: BlackholeConfig,
    acker: Acker,
    last: Option<Instant>,
}

impl BlackholeSink {
    pub fn new(config: BlackholeConfig, acker: Acker) -> Self {
        BlackholeSink {
            config,
            total_events: Arc::new(AtomicUsize::new(0)),
            total_raw_bytes: Arc::new(AtomicUsize::new(0)),
            acker,
            last: None,
        }
    }
}

#[async_trait]
impl StreamSink<EventArray> for BlackholeSink {
    async fn run(mut self: Box<Self>, mut input: BoxStream<'_, EventArray>) -> Result<(), ()> {
        // Spin up a task that does the periodic reporting.  This is decoupled from the main sink so
        // that rate limiting support can be added more simply without having to interleave it with
        // the printing.
        let total_events = Arc::clone(&self.total_events);
        let total_raw_bytes = Arc::clone(&self.total_raw_bytes);
        let (shutdown, mut tripwire) = watch::channel(());

        if self.config.print_interval_secs > 0 {
            let interval_dur = Duration::from_secs(self.config.print_interval_secs);
            tokio::spawn(async move {
                let mut print_interval = interval(interval_dur);
                loop {
                    select! {
                        _ = print_interval.tick() => {
                            info!({
                                events = total_events.load(Ordering::Relaxed),
                                raw_bytes_collected = total_raw_bytes.load(Ordering::Relaxed),
                            }, "Total events collected");
                        },
                        _ = tripwire.changed() => break,
                    }
                }

                info!({
                    events = total_events.load(Ordering::Relaxed),
                    raw_bytes_collected = total_raw_bytes.load(Ordering::Relaxed)
                }, "Total events collected");
            });
        }

        while let Some(events) = input.next().await {
            if let Some(rate) = self.config.rate {
                let factor: f32 = 1.0 / rate as f32;
                let secs: f32 = factor * (events.len() as f32);
                let until = self.last.unwrap_or_else(Instant::now) + Duration::from_secs_f32(secs);
                sleep_until(until.into()).await;
                self.last = Some(until);
            }

            let message_len = events.size_of();

            let _ = self.total_events.fetch_add(events.len(), Ordering::AcqRel);
            let _ = self
                .total_raw_bytes
                .fetch_add(message_len, Ordering::AcqRel);

            emit!(EventsSent {
                count: events.len(),
                byte_size: message_len,
                output: None,
            });

            self.acker.ack(events.len());
        }

        // Notify the reporting task to shutdown.
        let _ = shutdown.send(());

        Ok(())
    }
}
