package metadata

components: sinks: nats: {
	title: "NATS"

	classes: {
		commonly_used: false
		delivery:      "best_effort"
		development:   "beta"
		egress_method: "stream"
		service_providers: []
		stateful: false
	}

	features: {
		acknowledgements: false
		healthcheck: enabled: true
		send: {
			compression: enabled: false
			encoding: {
				enabled: true
				codec: {
					enabled: true
					enum: ["json", "text"]
				}
			}
			request: enabled: false
			tls: {
				enabled:                true
				can_verify_certificate: true
				can_verify_hostname:    true
				enabled_default:        false
			}
			to: {
				service: services.nats

				interface: {
					socket: {
						direction: "outgoing"
						protocols: ["tcp"]
						ssl: "disabled"
					}
				}
			}
		}
	}

	support: {
		requirements: []
		warnings: []
		notices: []
	}

	configuration: components._nats.configuration & {}

	input: {
		logs:    true
		metrics: null
		traces:  false
	}

	how_it_works: components._nats.how_it_works

	telemetry: metrics: {
		events_discarded_total:  components.sources.internal_metrics.output.metrics.events_discarded_total
		processing_errors_total: components.sources.internal_metrics.output.metrics.processing_errors_total
		processed_bytes_total:   components.sources.internal_metrics.output.metrics.processed_bytes_total
		processed_events_total:  components.sources.internal_metrics.output.metrics.processed_events_total
		send_errors_total:       components.sources.internal_metrics.output.metrics.send_errors_total
	}
}
