use metrics::counter;
use vector_core::internal_event::InternalEvent;

use super::prelude::{error_stage, error_type};

#[derive(Debug)]
pub struct CoercerConversionError<'a> {
    pub field: &'a str,
    pub error: crate::types::Error,
}

impl<'a> InternalEvent for CoercerConversionError<'a> {
    fn emit(self) {
        error!(
            message = "Could not convert types.",
            field = %self.field,
            error = %self.error,
            error_type = error_type::CONVERSION_FAILED,
            stage = error_stage::PROCESSING,
            internal_log_rate_secs = 30
        );
        counter!(
            "component_errors_total", 1,
            "error_type" => error_type::CONVERSION_FAILED,
            "stage" => error_stage::PROCESSING,
            "field" => self.field.to_string(),
        );
        // deprecated
        counter!("processing_errors_total", 1, "error_type" => "type_conversion_failed");
    }
}
