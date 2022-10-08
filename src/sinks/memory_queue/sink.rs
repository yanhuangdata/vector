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
    event::{EventArray, EventContainer, Event},
    sinks::{memory_queue::config::MemoryQueueConfig, util::StreamSink},
    topology::{GLOBAL_VEC_TX},
};

pub struct MemoryQueueSink {
    total_events: Arc<AtomicUsize>,
    total_raw_bytes: Arc<AtomicUsize>,
    config: MemoryQueueConfig,
    acker: Acker,
    last: Option<Instant>,
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
impl StreamSink<EventArray> for MemoryQueueSink {
    async fn run(mut self: Box<Self>, mut input: BoxStream<'_, EventArray>) -> Result<(), ()> {
        // Spin up a task that does the periodic reporting.  This is decoupled from the main sink so
        // that rate limiting support can be added more simply without having to interleave it with
        // the printing.
        let total_events = Arc::clone(&self.total_events);
        let total_raw_bytes = Arc::clone(&self.total_raw_bytes);
        let (shutdown, mut tripwire) = watch::channel(());

        while let Some(events) = input.next().await {
            // if let Some(rate) = self.config.rate {
            //     let factor: f32 = 1.0 / rate as f32;
            //     let secs: f32 = factor * (events.len() as f32);
            //     let until = self.last.unwrap_or_else(Instant::now) + Duration::from_secs_f32(secs);
            //     sleep_until(until.into()).await;
            //     self.last = Some(until);
            // }

            // let message_len = events.size_of();

            // let _ = self.total_events.fetch_add(events.len(), Ordering::AcqRel);
            // let _ = self
            //     .total_raw_bytes
            //     .fetch_add(message_len, Ordering::AcqRel);

            // emit!(EventsSent {
            //     count: events.len(),
            //     byte_size: message_len,
            //     output: None,
            // });

            let size = events.len();

            unsafe {
                if let Some(sender) = &mut GLOBAL_VEC_TX {
                    match events {
                        EventArray::Logs(logs) => {
                            let mut log_events = Vec::with_capacity(size);
                            // for ev in logs.iter() {
                            //     log_events.push(Event::from(*ev));
                            // }
                            for event in logs.into_events() {
                                log_events.push(event);
                            }
                            // sender.try_send(events.clone());
                            // we use a MPSC channel here, and set the buffer size to a limited count, which make sender
                            // only outpaces receiver by the limited count. When sender is full, we sleep and retry, and
                            // do not ack the source, to make source buffered on disk if source producing is too fast.
                            while let Err(send_err) = sender.try_send(log_events.clone()) {
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
                        },
                        _ => {}
                    }

                }
            }
            self.acker.ack(size);

        }

        // Notify the reporting task to shutdown.
        let _ = shutdown.send(());

        Ok(())
    }
}

// impl StreamSink<Event> for MemoryQueueSink {
//     async fn run(mut self: Box<Self>, mut input: BoxStream<'_, Event>) -> Result<(), ()> {
//         // Spin up a task that does the periodic reporting.  This is decoupled from the main sink so
//         // that rate limiting support can be added more simply without having to interleave it with
//         // the printing.
//         let total_events = Arc::clone(&self.total_events);
//         let total_raw_bytes = Arc::clone(&self.total_raw_bytes);
//         let (shutdown, mut tripwire) = watch::channel(());
//
//         // let mut chunks = input.ready_chunks(1024);
//
//         // if self.config.print_interval_secs > 0 {
//         //     let interval_dur = Duration::from_secs(self.config.print_interval_secs);
//         //     tokio::spawn(async move {
//         //         let mut print_interval = interval(interval_dur);
//         //         loop {
//         //             select! {
//         //                 _ = print_interval.tick() => {
//         //                     info!({
//         //                         events = total_events.load(Ordering::Relaxed),
//         //                         raw_bytes_collected = total_raw_bytes.load(Ordering::Relaxed),
//         //                     }, "Total events collected");
//         //                 },
//         //                 _ = tripwire.changed() => break,
//         //             }
//         //         }
//
//         //         info!({
//         //             events = total_events.load(Ordering::Relaxed),
//         //             raw_bytes_collected = total_raw_bytes.load(Ordering::Relaxed)
//         //         }, "Total events collected");
//         //     });
//         // }
//         let mut count: u32 = 0;
//         let mut log_events = Vec::new();
//
//         unsafe {
//             if let Some(sender) = &mut GLOBAL_VEC_TX {
//                 while let Some(mut event) = input.next().await {
//                     log_events.push(event);
//                     count += 1;
//                     if count >= 1024 {
//                         println!("1024!!, {:?}", count);
//                         while let Err(send_err) = sender.try_send(log_events.clone()) {
//                             if send_err.is_full() {
//                                 // queue is full, waiting
//                                 let sleep_time = std::time::Duration::from_millis(50);
//                                 std::thread::sleep(sleep_time);
//                             } else if send_err.is_disconnected() {
//                                 error!("failed to send events to memory queue due to disconnected error");
//                             } else {
//                                 break;
//                             }
//                         }
//                         log_events.clear();
//                         count = 0;
//                     }
//
//                     self.acker.ack(1);
//
//                 }
//             }
//         }
//
//         // Notify the reporting task to shutdown.
//         let _ = shutdown.send(());
//
//         Ok(())
//     }
// }
