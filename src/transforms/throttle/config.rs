use std::time::Duration;

use governor::clock;
use serde_with::serde_as;
use vector_lib::{
    config::{LogNamespace, clone_input_definitions},
    configurable::configurable_component,
};

use super::transform::Throttle;
use crate::{
    conditions::AnyCondition,
    config::{DataType, Input, OutputId, TransformConfig, TransformContext, TransformOutput},
    schema,
    template::Template,
    transforms::Transform,
};

/// Configuration of internal metrics for the Throttle transform.
#[configurable_component]
#[derive(Clone, Debug, PartialEq, Eq, Default)]
#[serde(deny_unknown_fields)]
pub struct ThrottleInternalMetricsConfig {
    /// Whether or not to emit the `events_discarded_total` internal metric with the `key` tag.
    ///
    /// If true, the counter will be incremented for each discarded event, including the key value
    /// associated with the discarded event. If false, the counter will not be emitted. Instead, the
    /// number of discarded events can be seen through the `component_discarded_events_total` internal
    /// metric.
    ///
    /// Note that this defaults to false because the `key` tag has potentially unbounded cardinality.
    /// Only set this to true if you know that the number of unique keys is bounded.
    #[serde(default)]
    pub emit_events_discarded_per_key: bool,
}

/// Configuration for the `throttle` transform.
#[configurable_component]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum ThrottleLimitType {
    /// Rate limit based on event count.
    #[default]
    Event,

    /// Rate limit based on the estimated JSON-encoded size of events.
    Byte,
}

/// Configuration for the `throttle` transform.
#[serde_as]
#[configurable_component(transform("throttle", "Rate limit logs passing through a topology."))]
#[derive(Clone, Debug, Default)]
#[serde(deny_unknown_fields)]
pub struct ThrottleConfig {
    /// The number of units allowed for a given bucket per configured `window_secs`.
    ///
    /// Each unique key has its own `threshold`.
    ///
    /// The unit is determined by `limit_type`.
    pub threshold: u32,

    /// The unit used for rate limiting.
    #[serde(default)]
    pub limit_type: ThrottleLimitType,

    /// The time window in which the configured `threshold` is applied, in seconds.
    #[serde_as(as = "serde_with::DurationSecondsWithFrac<f64>")]
    #[configurable(metadata(docs::human_name = "Time Window"))]
    pub window_secs: Duration,

    /// The value to group events into separate buckets to be rate limited independently.
    ///
    /// If left unspecified, or if the event doesn't have `key_field`, then the event is not rate
    /// limited separately.
    #[configurable(metadata(docs::examples = "{{ message }}", docs::examples = "{{ hostname }}",))]
    pub key_field: Option<Template>,

    /// A logical condition used to exclude events from sampling.
    pub exclude: Option<AnyCondition>,

    #[configurable(derived)]
    #[serde(default)]
    pub internal_metrics: ThrottleInternalMetricsConfig,
}

impl_generate_config_from_default!(ThrottleConfig);

#[async_trait::async_trait]
#[typetag::serde(name = "throttle")]
impl TransformConfig for ThrottleConfig {
    async fn build(&self, context: &TransformContext) -> crate::Result<Transform> {
        Throttle::new(self, context, clock::MonotonicClock).map(Transform::event_task)
    }

    fn input(&self) -> Input {
        Input::log()
    }

    fn outputs(
        &self,
        _: vector_lib::enrichment::TableRegistry,
        input_definitions: &[(OutputId, schema::Definition)],
        _: LogNamespace,
    ) -> Vec<TransformOutput> {
        // The event is not modified, so the definition is passed through as-is
        vec![TransformOutput::new(
            DataType::Log,
            clone_input_definitions(input_definitions),
        )]
    }
}

#[cfg(test)]
mod tests {
    use super::{ThrottleConfig, ThrottleLimitType};

    #[test]
    fn generate_config() {
        crate::test_util::test_generate_config::<ThrottleConfig>();
    }

    #[test]
    fn defaults_limit_type_to_event() {
        let config = toml::from_str::<ThrottleConfig>(
            r#"
threshold = 2
window_secs = 5
"#,
        )
        .unwrap();

        assert_eq!(config.limit_type, ThrottleLimitType::Event);
    }
}
