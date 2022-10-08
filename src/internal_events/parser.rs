use std::borrow::Cow;

use metrics::counter;
use vector_core::internal_event::InternalEvent;

use super::prelude::{error_stage, error_type};

fn truncate_string_at(s: &str, maxlen: usize) -> Cow<str> {
    let ellipsis: &str = "[...]";
    if s.len() >= maxlen {
        let mut len = maxlen - ellipsis.len();
        while !s.is_char_boundary(len) {
            len -= 1;
        }
        format!("{}{}", &s[..len], ellipsis).into()
    } else {
        s.into()
    }
}

#[derive(Debug)]
pub struct ParserMatchError<'a> {
    pub value: &'a [u8],
}

impl InternalEvent for ParserMatchError<'_> {
    fn emit(self) {
        error!(
            message = "Pattern failed to match.",
            error_code = "no_match_found",
            error_type = error_type::CONDITION_FAILED,
            stage = error_stage::PROCESSING,
            field = &truncate_string_at(&String::from_utf8_lossy(self.value), 60)[..],
            internal_log_rate_secs = 30
        );
        counter!(
            "component_errors_total", 1,
            "error_code" => "no_match_found",
            "error_type" => error_type::CONDITION_FAILED,
            "stage" => error_stage::PROCESSING,
        );
        // deprecated
        counter!("processing_errors_total", 1, "error_type" => "failed_match");
    }
}

#[derive(Debug)]
pub struct ParserMissingFieldError<'a> {
    pub field: &'a str,
}

impl InternalEvent for ParserMissingFieldError<'_> {
    fn emit(self) {
        error!(
            message = "Field does not exist.",
            field = %self.field,
            error_code = "field_not_found",
            error_type = error_type::CONDITION_FAILED,
            stage = error_stage::PROCESSING,
            internal_log_rate_secs = 10
        );
        counter!(
            "component_errors_total", 1,
            "error_code" => "field_not_found",
            "error_type" => error_type::CONDITION_FAILED,
            "stage" => error_stage::PROCESSING,
            "field" => self.field.to_string(),
        );
        // deprecated
        counter!("processing_errors_total", 1, "error_type" => "missing_field");
    }
}

#[derive(Debug)]
pub struct ParserTargetExistsError<'a> {
    pub target_field: &'a str,
}

impl<'a> InternalEvent for ParserTargetExistsError<'a> {
    fn emit(self) {
        error!(
            message = format!("Target field {:?} already exists.", self.target_field).as_str(),
            error_code = "target_field_exists",
            error_type = error_type::CONDITION_FAILED,
            stage = error_stage::PROCESSING,
            target_field = %self.target_field,
            internal_log_rate_secs = 10
        );
        counter!(
            "component_errors_total", 1,
            "error_code" => "target_field_exists",
            "error_type" => error_type::CONDITION_FAILED,
            "stage" => error_stage::PROCESSING,
            "target_field" => self.target_field.to_string(),
        );
        // deprecated
        counter!("processing_errors_total", 1, "error_type" => "target_field_exists");
    }
}

#[derive(Debug)]
pub struct ParserConversionError<'a> {
    pub name: &'a str,
    pub error: crate::types::Error,
}

impl<'a> InternalEvent for ParserConversionError<'a> {
    fn emit(self) {
        error!(
            message = "Could not convert types.",
            name = %self.name,
            error = ?self.error,
            error_code = "type_conversion",
            error_type = error_type::CONVERSION_FAILED,
            stage = error_stage::PROCESSING,
            internal_log_rate_secs = 30
        );
        counter!(
            "component_errors_total", 1,
            "error_code" => "type_conversion",
            "error_type" => error_type::CONVERSION_FAILED,
            "stage" => error_stage::PROCESSING,
            "name" => self.name.to_string(),
        );
        // deprecated
        counter!("processing_errors_total", 1, "error_type" => "type_conversion_failed");
    }
}

#[cfg(test)]
mod test {
    #[test]
    fn truncate_utf8() {
        let message = "Hello 😁 this is test.";
        assert_eq!("Hello [...]", super::truncate_string_at(message, 13));
    }
}
