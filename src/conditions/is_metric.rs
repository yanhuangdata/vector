use serde::{Deserialize, Serialize};

use crate::{
    conditions::{Condition, ConditionConfig, ConditionDescription, Conditional},
    event::Event,
};

//------------------------------------------------------------------------------

#[derive(Deserialize, Serialize, Debug, Default, Clone)]
pub(crate) struct IsMetricConfig {}

inventory::submit! {
    ConditionDescription::new::<IsMetricConfig>("is_metric")
}

impl_generate_config_from_default!(IsMetricConfig);

#[typetag::serde(name = "is_metric")]
impl ConditionConfig for IsMetricConfig {
    fn build(&self, _enrichment_tables: &enrichment::TableRegistry) -> crate::Result<Condition> {
        Ok(Condition::is_metric())
    }
}

//------------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct IsMetric {}

impl Conditional for IsMetric {
    fn check(&self, e: &Event) -> bool {
        matches!(e, Event::Metric(_))
    }

    fn check_with_context(&self, e: &Event) -> Result<(), String> {
        if self.check(e) {
            Ok(())
        } else {
            Err("event is not a metric type".to_string())
        }
    }
}

//------------------------------------------------------------------------------

#[cfg(test)]
mod test {
    use super::*;
    use crate::event::{
        metric::{Metric, MetricKind, MetricValue},
        Event,
    };

    #[test]
    fn generate_config() {
        crate::test_util::test_generate_config::<IsMetricConfig>();
    }

    #[test]
    fn is_metric_basic() {
        let cond = IsMetricConfig {}.build(&Default::default()).unwrap();

        assert!(!cond.check(&Event::from("just a log")));
        assert!(cond.check(&Event::from(Metric::new(
            "test metric",
            MetricKind::Incremental,
            MetricValue::Counter { value: 1.0 },
        ))),);
    }
}
