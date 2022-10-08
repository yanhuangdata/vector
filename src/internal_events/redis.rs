use metrics::counter;
use vector_core::internal_event::InternalEvent;

use super::prelude::{error_stage, error_type};

#[derive(Debug)]
pub struct RedisSendEventError<'a> {
    error: &'a redis::RedisError,
    error_code: String,
}

#[cfg(feature = "sinks-redis")]
impl<'a> RedisSendEventError<'a> {
    pub fn new(error: &'a redis::RedisError) -> Self {
        Self {
            error,
            error_code: error.code().unwrap_or("UNKNOWN").to_string(),
        }
    }
}

impl<'a> InternalEvent for RedisSendEventError<'a> {
    fn emit(self) {
        error!(
            message = "Failed to send message.",
            error = %self.error,
            error_code = %self.error_code,
            error_type = error_type::WRITER_FAILED,
            stage = error_stage::SENDING,
            rate_limit_secs = 10,
        );
        counter!(
            "component_errors_total", 1,
            "error_code" => self.error_code,
            "error_type" => error_type::WRITER_FAILED,
            "stage" => error_stage::SENDING,
        );
        // deprecated
        counter!("send_errors_total", 1);
    }
}

#[derive(Debug)]
pub struct RedisReceiveEventError {
    error: redis::RedisError,
    error_code: String,
}

impl From<redis::RedisError> for RedisReceiveEventError {
    fn from(error: redis::RedisError) -> Self {
        let error_code = error.code().unwrap_or("UNKNOWN").to_string();
        Self { error, error_code }
    }
}

impl InternalEvent for RedisReceiveEventError {
    fn emit(self) {
        error!(
            message = "Failed to read message.",
            error = %self.error,
            error_code = %self.error_code,
            error_type = error_type::READER_FAILED,
            stage = error_stage::SENDING,
            rate_limit_secs = 10,
        );
        counter!(
            "component_errors_total", 1,
            "error_code" => self.error_code,
            "error_type" => error_type::READER_FAILED,
            "stage" => error_stage::RECEIVING,
        );
    }
}
