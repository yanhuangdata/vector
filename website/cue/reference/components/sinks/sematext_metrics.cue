package metadata

components: sinks: sematext_metrics: {
	title: "Sematext Metrics"

	classes: {
		commonly_used: false
		delivery:      "at_least_once"
		development:   "beta"
		service_providers: ["Sematext"]
		egress_method: "batch"
		stateful:      false
	}

	features: {
		acknowledgements: true
		healthcheck: enabled: true
		send: {
			batch: {
				enabled:      true
				common:       false
				max_events:   20
				timeout_secs: 1.0
			}
			compression: enabled: false
			encoding: {
				enabled: true
				codec: enabled: false
			}
			proxy: enabled:   true
			request: enabled: false
			tls: enabled:     false
			to: sinks._sematext.features.send.to
		}
	}

	support: {
		requirements: []
		warnings: [
			"""
				[Sematext monitoring](\(urls.sematext_monitoring)) only accepts metrics which contain a single value.
				Therefore, only `counter` and `gauge` metrics are supported. If you'd like to ingest other
				metric types please consider using the [`metric_to_log` transform](\(urls.vector_transforms)/metric_to_log)
				with the `sematext_logs` sink.
				""",
		]
		notices: []
	}

	configuration: sinks._sematext.configuration & {
		default_namespace: {
			description: "Used as a namespace for metrics that don't have it."
			required:    true
			warnings: []
			type: string: {
				examples: ["service"]
			}
		}
	}

	input: {
		logs: false
		metrics: {
			counter:      true
			distribution: false
			gauge:        true
			histogram:    false
			set:          false
			summary:      false
		}
		traces: false
	}

	telemetry: metrics: {
		component_sent_events_total:      components.sources.internal_metrics.output.metrics.component_sent_events_total
		component_sent_event_bytes_total: components.sources.internal_metrics.output.metrics.component_sent_event_bytes_total
		encode_errors_total:              components.sources.internal_metrics.output.metrics.encode_errors_total
		processing_errors_total:          components.sources.internal_metrics.output.metrics.processing_errors_total
	}
}
