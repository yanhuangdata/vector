use metrics::counter;
use vector_core::internal_event::InternalEvent;

use super::prelude::{error_stage, error_type};
use crate::event::metric::Metric;

#[derive(Debug)]
pub struct SematextMetricsInvalidMetricError<'a> {
    pub metric: &'a Metric,
}

impl<'a> InternalEvent for SematextMetricsInvalidMetricError<'a> {
    fn emit(self) {
        error!(
            message = "Invalid metric received; dropping event.",
            error_code = "invalid_metric",
            error_type =  error_type::ENCODER_FAILED,
            stage = error_stage::PROCESSING,
            value = ?self.metric.value(),
            kind = ?self.metric.kind(),
            internal_log_rate_secs = 10,
        );
        counter!(
            "component_errors_total", 1,
            "error_code" => "invalid_metric",
            "error_type" => error_type::ENCODER_FAILED,
            "stage" => error_stage::PROCESSING,
        );
        // deprecated
        counter!("processing_errors_total", 1);
    }
}

#[derive(Debug)]
pub struct SematextMetricsEncodeEventError<E> {
    pub error: E,
}

impl<E: std::fmt::Display> InternalEvent for SematextMetricsEncodeEventError<E> {
    fn emit(self) {
        error!(
            message = "Failed to encode event; dropping event.",
            error = %self.error,
            error_type = error_type::ENCODER_FAILED,
            stage = error_stage::PROCESSING,
            internal_log_rate_secs = 10,
        );
        counter!(
            "component_errors_total", 1,
            "error_type" => error_type::ENCODER_FAILED,
            "stage" => error_stage::PROCESSING,
        );
        // deprecated
        counter!("encode_errors_total", 1);
    }
}
