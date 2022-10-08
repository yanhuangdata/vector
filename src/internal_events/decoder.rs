use metrics::counter;
use vector_core::internal_event::InternalEvent;

#[derive(Debug)]
pub struct DecoderFramingFailed<'a> {
    pub error: &'a codecs::decoding::BoxedFramingError,
}

impl<'a> InternalEvent for DecoderFramingFailed<'a> {
    fn emit(self) {
        warn!(message = "Failed framing bytes.", error = %self.error, internal_log_rate_secs = 10);
        counter!("decoder_framing_errors_total", 1);
    }
}

#[derive(Debug)]
pub struct DecoderDeserializeFailed<'a> {
    pub error: &'a crate::Error,
}

impl<'a> InternalEvent for DecoderDeserializeFailed<'a> {
    fn emit(self) {
        warn!(message = "Failed deserializing frame.", error = %self.error, internal_log_rate_secs = 10);
        counter!("decoder_deserialize_errors_total", 1);
    }
}

#[derive(Debug)]
pub struct EncoderFramingFailed<'a> {
    pub error: &'a codecs::encoding::BoxedFramingError,
}

impl<'a> InternalEvent for EncoderFramingFailed<'a> {
    fn emit(self) {
        warn!(message = "Failed framing bytes.", error = %self.error, internal_log_rate_secs = 10);
        counter!("encoder_framing_errors_total", 1);
    }
}

#[derive(Debug)]
pub struct EncoderSerializeFailed<'a> {
    pub error: &'a crate::Error,
}

impl<'a> InternalEvent for EncoderSerializeFailed<'a> {
    fn emit(self) {
        warn!(message = "Failed serializing frame.", error = %self.error, internal_log_rate_secs = 10);
        counter!("encoder_serialize_errors_total", 1);
    }
}
