#[cfg(all(
    feature = "sinks-blackhole",
    feature = "sources-stdin",
    feature = "transforms-json_parser"
))]
mod transient_state;

#[cfg(all(feature = "sinks-console", feature = "sources-demo_logs"))]
mod source_finished;

#[cfg(all(
    feature = "sinks-console",
    feature = "sources-splunk_hec",
    feature = "sources-demo_logs",
    feature = "sinks-prometheus",
    feature = "transforms-log_to_metric",
    feature = "sinks-socket",
))]
mod reload;

#[cfg(all(feature = "sinks-console", feature = "sources-socket"))]
mod doesnt_reload;

mod backpressure;
mod compliance;
