use metrics::counter;
use vector_core::internal_event::InternalEvent;

#[derive(Debug)]
pub(crate) struct ThrottleEventDiscarded {
    pub key: String,
}

impl InternalEvent for ThrottleEventDiscarded {
    fn emit(self) {
        debug!(message = "Rate limit exceeded.", key = ?self.key);
        counter!(
            "events_discarded_total", 1,
            "key" => self.key,
        );
    }
}
