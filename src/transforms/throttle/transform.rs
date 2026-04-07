use std::{hash::Hash, num::NonZeroU32, pin::Pin, time::Duration};

use async_stream::stream;
use futures::{Stream, StreamExt};
use governor::{Quota, clock};
use snafu::Snafu;

use super::{
    config::{
        ThrottleConfig, ThrottleExceededAction, ThrottleInternalMetricsConfig, ThrottleLimitType,
    },
    rate_limiter::RateLimiterRunner,
};
use crate::{
    conditions::Condition,
    config::TransformContext,
    event::{EstimatedJsonEncodedSizeOf, Event},
    internal_events::{TemplateRenderingError, ThrottleEventDiscarded},
    template::Template,
    transforms::TaskTransform,
};

#[derive(Clone)]
pub struct Throttle<C: clock::Clock<Instant = I>, I: clock::Reference> {
    pub quota: Quota,
    pub flush_keys_interval: Duration,
    key_field: Option<Template>,
    exclude: Option<Condition>,
    limit_type: ThrottleLimitType,
    exceeded_action: ThrottleExceededAction,
    pub clock: C,
    internal_metrics: ThrottleInternalMetricsConfig,
}

impl<C, I> Throttle<C, I>
where
    C: clock::Clock<Instant = I> + Clone + Send + Sync + 'static,
    I: clock::Reference,
{
    pub fn new(
        config: &ThrottleConfig,
        context: &TransformContext,
        clock: C,
    ) -> crate::Result<Self> {
        let flush_keys_interval = config.window_secs;

        let threshold = match NonZeroU32::new(config.threshold) {
            Some(threshold) => threshold,
            None => return Err(Box::new(ConfigError::NonZero)),
        };

        let quota = match Quota::with_period(Duration::from_secs_f64(
            flush_keys_interval.as_secs_f64() / f64::from(threshold.get()),
        )) {
            Some(quota) => quota.allow_burst(threshold),
            None => return Err(Box::new(ConfigError::NonZero)),
        };
        let exclude = config
            .exclude
            .as_ref()
            .map(|condition| condition.build(&context.enrichment_tables))
            .transpose()?;

        Ok(Self {
            quota,
            clock,
            flush_keys_interval,
            key_field: config.key_field.clone(),
            exclude,
            limit_type: config.limit_type,
            exceeded_action: config.exceeded_action,
            internal_metrics: config.internal_metrics.clone(),
        })
    }

    #[must_use]
    pub fn start_rate_limiter<K>(&self) -> RateLimiterRunner<K, C>
    where
        K: Hash + Eq + Clone + Send + Sync + 'static,
    {
        RateLimiterRunner::start(self.quota, self.clock.clone(), self.flush_keys_interval)
    }

    pub fn emit_event_discarded(&self, key: String) {
        emit!(ThrottleEventDiscarded {
            key,
            emit_events_discarded_per_key: self.internal_metrics.emit_events_discarded_per_key
        });
    }

    fn permits_for(&self, event: &Event) -> Option<NonZeroU32> {
        match self.limit_type {
            ThrottleLimitType::Event => NonZeroU32::new(1),
            ThrottleLimitType::Byte => {
                let byte_size = u32::try_from(event.estimated_json_encoded_size_of().get()).ok()?;
                NonZeroU32::new(byte_size.max(1))
            }
        }
    }
}

impl<C, I> TaskTransform<Event> for Throttle<C, I>
where
    C: clock::Clock<Instant = I> + Clone + Send + Sync + 'static,
    I: clock::Reference + Send + 'static,
{
    fn transform(
        self: Box<Self>,
        mut input_rx: Pin<Box<dyn Stream<Item = Event> + Send>>,
    ) -> Pin<Box<dyn Stream<Item = Event> + Send>>
    where
        Self: 'static,
    {
        let limiter = self.start_rate_limiter();

        Box::pin(stream! {
            while let Some(event) = input_rx.next().await {
                let (throttle, event) = match self.exclude.as_ref() {
                    Some(condition) => {
                        let (result, event) = condition.check(event);
                        (!result, event)
                    },
                    _ => (true, event)
                };
                let output = if throttle {
                    let key = self.key_field.as_ref().and_then(|t| {
                        t.render_string(&event)
                            .map_err(|error| {
                                emit!(TemplateRenderingError {
                                    error,
                                    field: Some("key_field"),
                                    drop_event: false,
                                })
                            })
                            .ok()
                    });
                    let permits = self.permits_for(&event);

                    match (self.exceeded_action, permits) {
                        (_, Some(permits)) if limiter.check_key_n(&key, permits) => Some(event),
                        (ThrottleExceededAction::Drop, _) | (_, None) => {
                            self.emit_event_discarded(key.unwrap_or_else(|| "None".to_string()));
                            None
                        }
                        (ThrottleExceededAction::Block, Some(permits)) => {
                            if limiter.until_key_n_ready(&key, permits).await.is_ok() {
                                Some(event)
                            } else {
                                self.emit_event_discarded(key.unwrap_or_else(|| "None".to_string()));
                                None
                            }
                        }
                    }
                } else {
                    Some(event)
                };
                if let Some(event) = output {
                    yield event;
                }
            }
        })
    }
}

#[derive(Debug, Snafu)]
pub enum ConfigError {
    #[snafu(display("`threshold`, and `window_secs` must be non-zero"))]
    NonZero,
}

#[cfg(test)]
mod tests {
    use std::task::Poll;

    use futures::SinkExt;
    use tokio::sync::mpsc;
    use tokio_stream::wrappers::ReceiverStream;

    use super::*;
    use crate::{
        event::{EstimatedJsonEncodedSizeOf, LogEvent},
        test_util::components::assert_transform_compliance,
        transforms::{Transform, test::create_topology},
    };

    fn log_event_with_message(message: &str) -> Event {
        LogEvent::from_str_legacy(message).into()
    }

    fn log_event_with_field(message: &str, field: &str, value: &str) -> Event {
        let mut log = LogEvent::from_str_legacy(message);
        log.insert(field, value);
        log.into()
    }

    #[tokio::test]
    async fn throttle_events() {
        let clock = clock::FakeRelativeClock::default();
        let config = toml::from_str::<ThrottleConfig>(
            r"
threshold = 2
window_secs = 5
",
        )
        .unwrap();

        let throttle = Throttle::new(&config, &TransformContext::default(), clock.clone())
            .map(Transform::event_task)
            .unwrap();

        let throttle = throttle.into_task();

        let (mut tx, rx) = futures::channel::mpsc::channel(10);
        let mut out_stream = throttle.transform_events(Box::pin(rx));

        // tokio interval is always immediately ready, so we poll once to make sure
        // we trip it/set the interval in the future
        assert_eq!(Poll::Pending, futures::poll!(out_stream.next()));

        tx.send(LogEvent::default().into()).await.unwrap();
        tx.send(LogEvent::default().into()).await.unwrap();

        let mut count = 0_u8;
        while count < 2 {
            match out_stream.next().await {
                Some(_event) => {
                    count += 1;
                }
                _ => {
                    panic!("Unexpectedly received None in output stream");
                }
            }
        }
        assert_eq!(2, count);

        clock.advance(Duration::from_secs(2));

        tx.send(LogEvent::default().into()).await.unwrap();

        // We should be back to pending, having the second event dropped
        assert_eq!(Poll::Pending, futures::poll!(out_stream.next()));

        clock.advance(Duration::from_secs(3));

        tx.send(LogEvent::default().into()).await.unwrap();

        // The rate limiter should now be refreshed and allow an additional event through
        match out_stream.next().await {
            Some(_event) => {}
            _ => {
                panic!("Unexpectedly received None in output stream");
            }
        }

        // We should be back to pending, having nothing waiting for us
        assert_eq!(Poll::Pending, futures::poll!(out_stream.next()));

        tx.disconnect();

        // And still nothing there
        assert_eq!(Poll::Ready(None), futures::poll!(out_stream.next()));
    }

    #[tokio::test]
    async fn throttle_events_drop_action_drops_excess_event() {
        let clock = clock::FakeRelativeClock::default();
        let config = ThrottleConfig {
            threshold: 1,
            limit_type: ThrottleLimitType::Event,
            exceeded_action: ThrottleExceededAction::Drop,
            window_secs: Duration::from_secs(5),
            ..Default::default()
        };

        let throttle = Throttle::new(&config, &TransformContext::default(), clock.clone())
            .map(Transform::event_task)
            .unwrap();

        let throttle = throttle.into_task();

        let (mut tx, rx) = futures::channel::mpsc::channel(10);
        let mut out_stream = throttle.transform_events(Box::pin(rx));

        assert_eq!(Poll::Pending, futures::poll!(out_stream.next()));

        tx.send(LogEvent::default().into()).await.unwrap();
        match out_stream.next().await {
            Some(_event) => {}
            _ => panic!("Unexpectedly received None in output stream"),
        }

        tx.send(LogEvent::default().into()).await.unwrap();
        assert_eq!(Poll::Pending, futures::poll!(out_stream.next()));
    }

    #[tokio::test]
    async fn throttle_bytes() {
        let clock = clock::FakeRelativeClock::default();
        let event = log_event_with_message("byte-sized-event");
        let threshold = u32::try_from(event.estimated_json_encoded_size_of().get()).unwrap() * 2;
        let config = ThrottleConfig {
            threshold,
            limit_type: ThrottleLimitType::Byte,
            window_secs: Duration::from_secs(5),
            ..Default::default()
        };

        let throttle = Throttle::new(&config, &TransformContext::default(), clock.clone())
            .map(Transform::event_task)
            .unwrap();

        let throttle = throttle.into_task();

        let (mut tx, rx) = futures::channel::mpsc::channel(10);
        let mut out_stream = throttle.transform_events(Box::pin(rx));

        assert_eq!(Poll::Pending, futures::poll!(out_stream.next()));

        tx.send(event.clone()).await.unwrap();
        tx.send(event.clone()).await.unwrap();

        let mut count = 0_u8;
        while count < 2 {
            match out_stream.next().await {
                Some(_event) => {
                    count += 1;
                }
                _ => {
                    panic!("Unexpectedly received None in output stream");
                }
            }
        }
        assert_eq!(2, count);

        tx.send(event.clone()).await.unwrap();
        assert_eq!(Poll::Pending, futures::poll!(out_stream.next()));

        clock.advance(Duration::from_secs(5));

        tx.send(event).await.unwrap();
        match out_stream.next().await {
            Some(_event) => {}
            _ => {
                panic!("Unexpectedly received None in output stream");
            }
        }
    }

    #[tokio::test]
    async fn throttle_bytes_rejects_oversized_event() {
        let clock = clock::FakeRelativeClock::default();
        let event = log_event_with_message(&"x".repeat(128));
        let event_size = u32::try_from(event.estimated_json_encoded_size_of().get()).unwrap();
        let config = ThrottleConfig {
            threshold: event_size - 1,
            limit_type: ThrottleLimitType::Byte,
            window_secs: Duration::from_secs(5),
            ..Default::default()
        };

        let throttle = Throttle::new(&config, &TransformContext::default(), clock)
            .map(Transform::event_task)
            .unwrap();

        let throttle = throttle.into_task();

        let (mut tx, rx) = futures::channel::mpsc::channel(10);
        let mut out_stream = throttle.transform_events(Box::pin(rx));

        assert_eq!(Poll::Pending, futures::poll!(out_stream.next()));

        tx.send(event).await.unwrap();

        assert_eq!(Poll::Pending, futures::poll!(out_stream.next()));
    }

    #[tokio::test]
    async fn throttle_events_blocks_until_capacity_is_available() {
        let config = ThrottleConfig {
            threshold: 1,
            limit_type: ThrottleLimitType::Event,
            exceeded_action: ThrottleExceededAction::Block,
            window_secs: Duration::from_millis(25),
            ..Default::default()
        };

        let throttle = Throttle::new(&config, &TransformContext::default(), clock::MonotonicClock)
            .map(Transform::event_task)
            .unwrap();

        let throttle = throttle.into_task();

        let (mut tx, rx) = futures::channel::mpsc::channel(10);
        let mut out_stream = throttle.transform_events(Box::pin(rx));

        tx.send(LogEvent::default().into()).await.unwrap();
        tx.send(LogEvent::default().into()).await.unwrap();

        assert!(out_stream.next().await.is_some());
        assert!(out_stream.next().await.is_some());
    }

    #[tokio::test]
    async fn throttle_bytes_blocks_until_capacity_is_available() {
        let event = log_event_with_message("byte-sized-event");
        let threshold = u32::try_from(event.estimated_json_encoded_size_of().get()).unwrap();
        let config = ThrottleConfig {
            threshold,
            limit_type: ThrottleLimitType::Byte,
            exceeded_action: ThrottleExceededAction::Block,
            window_secs: Duration::from_millis(25),
            ..Default::default()
        };

        let throttle = Throttle::new(&config, &TransformContext::default(), clock::MonotonicClock)
            .map(Transform::event_task)
            .unwrap();

        let throttle = throttle.into_task();

        let (mut tx, rx) = futures::channel::mpsc::channel(10);
        let mut out_stream = throttle.transform_events(Box::pin(rx));

        tx.send(event.clone()).await.unwrap();
        tx.send(event).await.unwrap();

        assert!(out_stream.next().await.is_some());
        assert!(out_stream.next().await.is_some());
    }

    #[tokio::test]
    async fn throttle_bytes_block_drops_oversized_event() {
        let event = log_event_with_message(&"x".repeat(128));
        let event_size = u32::try_from(event.estimated_json_encoded_size_of().get()).unwrap();
        let config = ThrottleConfig {
            threshold: event_size - 1,
            limit_type: ThrottleLimitType::Byte,
            exceeded_action: ThrottleExceededAction::Block,
            window_secs: Duration::from_millis(25),
            ..Default::default()
        };

        let throttle = Throttle::new(&config, &TransformContext::default(), clock::MonotonicClock)
            .map(Transform::event_task)
            .unwrap();

        let throttle = throttle.into_task();

        let (mut tx, rx) = futures::channel::mpsc::channel(10);
        let mut out_stream = throttle.transform_events(Box::pin(rx));

        tx.send(event).await.unwrap();

        assert!(
            tokio::time::timeout(Duration::from_millis(50), out_stream.next())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn throttle_bytes_exclude() {
        let clock = clock::FakeRelativeClock::default();
        let event = log_event_with_message("byte-sized-event");
        let threshold = u32::try_from(event.estimated_json_encoded_size_of().get()).unwrap() * 2;
        let config = toml::from_str::<ThrottleConfig>(&format!(
            r#"
threshold = {threshold}
limit_type = "byte"
window_secs = 5
exclude = """
exists(.special)
"""
"#
        ))
        .unwrap();

        let throttle = Throttle::new(&config, &TransformContext::default(), clock.clone())
            .map(Transform::event_task)
            .unwrap();

        let throttle = throttle.into_task();

        let (mut tx, rx) = futures::channel::mpsc::channel(10);
        let mut out_stream = throttle.transform_events(Box::pin(rx));

        assert_eq!(Poll::Pending, futures::poll!(out_stream.next()));

        tx.send(event.clone()).await.unwrap();
        tx.send(event.clone()).await.unwrap();

        let mut count = 0_u8;
        while count < 2 {
            match out_stream.next().await {
                Some(_event) => count += 1,
                _ => panic!("Unexpectedly received None in output stream"),
            }
        }

        tx.send(event.clone()).await.unwrap();
        assert_eq!(Poll::Pending, futures::poll!(out_stream.next()));

        tx.send(log_event_with_field("excluded", "special", "true"))
            .await
            .unwrap();
        match out_stream.next().await {
            Some(_event) => {}
            _ => panic!("Unexpectedly received None in output stream"),
        }
    }

    #[tokio::test]
    async fn throttle_bytes_buckets() {
        let clock = clock::FakeRelativeClock::default();
        let event_a = log_event_with_field("byte-sized-event", "bucket", "a");
        let event_b = log_event_with_field("byte-sized-event", "bucket", "b");
        let threshold = u32::try_from(event_a.estimated_json_encoded_size_of().get()).unwrap();
        let config = toml::from_str::<ThrottleConfig>(&format!(
            r#"
threshold = {threshold}
limit_type = "byte"
window_secs = 5
key_field = "{{{{ bucket }}}}"
"#
        ))
        .unwrap();

        let throttle = Throttle::new(&config, &TransformContext::default(), clock)
            .map(Transform::event_task)
            .unwrap();

        let throttle = throttle.into_task();

        let (mut tx, rx) = futures::channel::mpsc::channel(10);
        let mut out_stream = throttle.transform_events(Box::pin(rx));

        assert_eq!(Poll::Pending, futures::poll!(out_stream.next()));

        tx.send(event_a).await.unwrap();
        tx.send(event_b).await.unwrap();

        let mut count = 0_u8;
        while count < 2 {
            match out_stream.next().await {
                Some(_event) => count += 1,
                _ => panic!("Unexpectedly received None in output stream"),
            }
        }

        assert_eq!(Poll::Pending, futures::poll!(out_stream.next()));
    }

    #[tokio::test]
    async fn throttle_exclude() {
        let clock = clock::FakeRelativeClock::default();
        let config = toml::from_str::<ThrottleConfig>(
            r#"
threshold = 2
window_secs = 5
exclude = """
exists(.special)
"""
"#,
        )
        .unwrap();

        let throttle = Throttle::new(&config, &TransformContext::default(), clock.clone())
            .map(Transform::event_task)
            .unwrap();

        let throttle = throttle.into_task();

        let (mut tx, rx) = futures::channel::mpsc::channel(10);
        let mut out_stream = throttle.transform_events(Box::pin(rx));

        // tokio interval is always immediately ready, so we poll once to make sure
        // we trip it/set the interval in the future
        assert_eq!(Poll::Pending, futures::poll!(out_stream.next()));

        tx.send(LogEvent::default().into()).await.unwrap();
        tx.send(LogEvent::default().into()).await.unwrap();

        let mut count = 0_u8;
        while count < 2 {
            match out_stream.next().await {
                Some(_event) => {
                    count += 1;
                }
                _ => {
                    panic!("Unexpectedly received None in output stream");
                }
            }
        }
        assert_eq!(2, count);

        clock.advance(Duration::from_secs(2));

        tx.send(LogEvent::default().into()).await.unwrap();

        // We should be back to pending, having the second event dropped
        assert_eq!(Poll::Pending, futures::poll!(out_stream.next()));

        let mut special_log = LogEvent::default();
        special_log.insert("special", "true");
        tx.send(special_log.into()).await.unwrap();
        // The rate limiter should allow this log through regardless of current limit
        match out_stream.next().await {
            Some(_event) => {}
            _ => {
                panic!("Unexpectedly received None in output stream");
            }
        }

        clock.advance(Duration::from_secs(3));

        tx.send(LogEvent::default().into()).await.unwrap();

        // The rate limiter should now be refreshed and allow an additional event through
        match out_stream.next().await {
            Some(_event) => {}
            _ => {
                panic!("Unexpectedly received None in output stream");
            }
        }

        // We should be back to pending, having nothing waiting for us
        assert_eq!(Poll::Pending, futures::poll!(out_stream.next()));

        tx.disconnect();

        // And still nothing there
        assert_eq!(Poll::Ready(None), futures::poll!(out_stream.next()));
    }

    #[tokio::test]
    async fn throttle_buckets() {
        let clock = clock::FakeRelativeClock::default();
        let config = toml::from_str::<ThrottleConfig>(
            r#"
threshold = 1
window_secs = 5
key_field = "{{ bucket }}"
"#,
        )
        .unwrap();

        let throttle = Throttle::new(&config, &TransformContext::default(), clock.clone())
            .map(Transform::event_task)
            .unwrap();

        let throttle = throttle.into_task();

        let (mut tx, rx) = futures::channel::mpsc::channel(10);
        let mut out_stream = throttle.transform_events(Box::pin(rx));

        // tokio interval is always immediately ready, so we poll once to make sure
        // we trip it/set the interval in the future
        assert_eq!(Poll::Pending, futures::poll!(out_stream.next()));

        let mut log_a = LogEvent::default();
        log_a.insert("bucket", "a");
        let mut log_b = LogEvent::default();
        log_b.insert("bucket", "b");
        tx.send(log_a.into()).await.unwrap();
        tx.send(log_b.into()).await.unwrap();

        let mut count = 0_u8;
        while count < 2 {
            match out_stream.next().await {
                Some(_event) => {
                    count += 1;
                }
                _ => {
                    panic!("Unexpectedly received None in output stream");
                }
            }
        }
        assert_eq!(2, count);

        // We should be back to pending, having nothing waiting for us
        assert_eq!(Poll::Pending, futures::poll!(out_stream.next()));

        tx.disconnect();

        // And still nothing there
        assert_eq!(Poll::Ready(None), futures::poll!(out_stream.next()));
    }

    #[tokio::test]
    async fn emits_internal_events() {
        assert_transform_compliance(async move {
            let config = ThrottleConfig {
                threshold: 1,
                limit_type: ThrottleLimitType::Event,
                exceeded_action: ThrottleExceededAction::Drop,
                window_secs: Duration::from_secs_f64(1.0),
                key_field: None,
                exclude: None,
                internal_metrics: Default::default(),
            };
            let (tx, rx) = mpsc::channel(1);
            let (topology, mut out) = create_topology(ReceiverStream::new(rx), config).await;

            let log = LogEvent::from("hello world");
            tx.send(log.into()).await.unwrap();

            _ = out.recv().await;

            drop(tx);
            topology.stop().await;
            assert_eq!(out.recv().await, None);
        })
        .await
    }
}
