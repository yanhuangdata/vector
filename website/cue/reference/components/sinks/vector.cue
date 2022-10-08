package metadata

components: sinks: vector: {
	_port: 6000

	title: "Vector"

	description: """
		Sends data to another downstream Vector instance via the Vector source.
		"""

	classes: {
		commonly_used: false
		delivery:      "best_effort"
		development:   "beta"
		egress_method: "batch"
		service_providers: []
		stateful: false
	}
	features: {
		acknowledgements: true
		healthcheck: enabled: true
		send: {
			batch: {
				enabled:      true
				common:       false
				max_bytes:    10_000_000
				timeout_secs: 1.0
			}
			compression: enabled:       false
			encoding: enabled:          false
			send_buffer_bytes: enabled: true
			keepalive: enabled:         true
			request: {
				enabled:       true
				headers:       false
				relevant_when: "version = \"2\""
			}

			tls: {
				enabled:                true
				can_verify_certificate: true
				can_verify_hostname:    true
				enabled_default:        false
			}
			to: {
				service: services.vector

				interface: {
					socket: {
						direction: "outgoing"
						protocols: ["http"]
						ssl: "optional"
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

	input: {
		logs: true
		metrics: {
			counter:      true
			distribution: true
			gauge:        true
			histogram:    true
			summary:      true
			set:          true
		}
		traces: false
	}

	configuration: {
		address: {
			description: "The downstream Vector address to connect to. The address _must_ include a port."
			required:    true
			type: string: {
				examples: ["92.12.333.224:\(_port)"]
			}
		}
		compression: {
			description: "Enable gRPC compression with gzip."
			common:      true
			required:    false
			type: bool: default: false
		}
		version: {
			description: "Sink API version. Specifying this version ensures that Vector does not break backward compatibility."
			common:      true
			required:    false
			warnings: ["Ensure you use the same version for both the sink and source."]
			type: string: {
				enum: {
					"1": "Vector sink API version 1"
					"2": "Vector sink API version 2"
				}
				default: "1"
			}
		}
	}

	how_it_works: components.sources.vector.how_it_works

	telemetry: metrics: {
		component_sent_bytes_total:       components.sources.internal_metrics.output.metrics.component_sent_bytes_total
		component_sent_events_total:      components.sources.internal_metrics.output.metrics.component_sent_events_total
		component_sent_event_bytes_total: components.sources.internal_metrics.output.metrics.component_sent_event_bytes_total
		processed_bytes_total:            components.sources.internal_metrics.output.metrics.processed_bytes_total
		processed_events_total:           components.sources.internal_metrics.output.metrics.processed_events_total
		protobuf_decode_errors_total:     components.sources.internal_metrics.output.metrics.protobuf_decode_errors_total
	}
}
