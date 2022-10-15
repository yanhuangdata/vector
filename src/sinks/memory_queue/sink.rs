use std::{
    sync::{
        atomic::{AtomicUsize},
        Arc,
    },
    time::{Duration, Instant},
};

use async_trait::async_trait;
use futures::{stream::BoxStream, StreamExt};
use tokio::{
    sync::watch,
    time::sleep_until,
};
use vector_core::event::{EventArray, EventContainer};

use crate::{
    topology::{GLOBAL_VEC_TX},
    sinks::{memory_queue::MemoryQueueConfig, util::StreamSink},
};

pub struct MemoryQueueSink {
    total_events: Arc<AtomicUsize>,
    total_raw_bytes: Arc<AtomicUsize>,
    config: MemoryQueueConfig,
    last: Option<Instant>,
}


impl MemoryQueueSink {
    pub fn new(config: MemoryQueueConfig) -> Self {
        MemoryQueueSink {
            config,
            total_events: Arc::new(AtomicUsize::new(0)),
            total_raw_bytes: Arc::new(AtomicUsize::new(0)),
            last: None,
        }
    }
}

#[async_trait]
impl StreamSink<EventArray> for MemoryQueueSink {
    async fn run(mut self: Box<Self>, mut input: BoxStream<'_, EventArray>) -> Result<(), ()> {
        let _total_events = Arc::clone(&self.total_events);
        let _total_raw_bytes = Arc::clone(&self.total_raw_bytes);
        let (shutdown, mut _tripwire) = watch::channel(());

        while let Some(events) = input.next().await {
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

            // BRIAN TODO: may do some refine on acker logic here
            // self.acker.ack(events.len());
        }

        // Notify the reporting task to shutdown.
        let _ = shutdown.send(());

        Ok(())
    }
}
