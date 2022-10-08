use metrics::counter;
use vector_core::internal_event::InternalEvent;

use super::prelude::{error_stage, error_type, io_error_code};

#[derive(Debug)]
pub struct HerokuLogplexRequestReceived<'a> {
    pub msg_count: usize,
    pub frame_id: &'a str,
    pub drain_token: &'a str,
}

impl<'a> InternalEvent for HerokuLogplexRequestReceived<'a> {
    fn emit(self) {
        info!(
            message = "Handling logplex request.",
            msg_count = %self.msg_count,
            frame_id = %self.frame_id,
            drain_token = %self.drain_token,
            internal_log_rate_secs = 10
        );
        counter!("requests_received_total", 1);
    }
}

#[derive(Debug)]
pub struct HerokuLogplexRequestReadError {
    pub error: std::io::Error,
}

impl InternalEvent for HerokuLogplexRequestReadError {
    fn emit(self) {
        error!(
            message = "Error reading request body.",
            error = ?self.error,
            internal_log_rate_secs = 10,
            error_type = error_type::READER_FAILED,
            error_code = io_error_code(&self.error),
            stage = error_stage::PROCESSING,
        );
        counter!(
            "component_errors_total", 1,
            "error_type" => error_type::READER_FAILED,
            "error_code" => io_error_code(&self.error),
            "stage" => error_stage::PROCESSING,
        );
        // deprecated
        counter!("request_read_errors_total", 1);
    }
}
