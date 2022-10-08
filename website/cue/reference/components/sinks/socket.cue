package metadata

components: sinks: socket: {
	title: "Socket"

	classes: {
		commonly_used: true
		delivery:      "best_effort"
		development:   "stable"
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
			send_buffer_bytes: {
				enabled:       true
				relevant_when: "mode = `tcp` or mode = `udp`"
			}
			keepalive: enabled: true
			request: enabled:   false
			tls: {
				enabled:                true
				can_verify_certificate: true
				can_verify_hostname:    true
				enabled_default:        false
			}
			to: {
				service: services.socket_receiver

				interface: {
					socket: {
						direction: "outgoing"
						protocols: ["tcp", "udp", "unix"]
						ssl: "required"
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

	configuration: {
		address: {
			description:   "The address to connect to. The address _must_ include a port."
			relevant_when: "mode = `tcp` or `udp`"
			required:      true
			type: string: {
				examples: ["92.12.333.224:5000"]
			}
		}
		mode: {
			description: "The type of socket to use."
			required:    true
			type: string: {
				enum: {
					tcp:  "TCP socket"
					udp:  "UDP socket"
					unix: "Unix domain socket"
				}
			}
		}
		path: {
			description:   "The unix socket path. This should be the absolute path."
			relevant_when: "mode = `unix`"
			required:      true
			type: string: {
				examples: ["/path/to/socket"]
			}
		}
	}

	input: {
		logs:    true
		metrics: null
		traces:  false
	}

	telemetry: metrics: {
		connection_errors_total: components.sources.internal_metrics.output.metrics.connection_errors_total
		processed_bytes_total:   components.sources.internal_metrics.output.metrics.processed_bytes_total
		processed_events_total:  components.sources.internal_metrics.output.metrics.processed_events_total
	}
}
