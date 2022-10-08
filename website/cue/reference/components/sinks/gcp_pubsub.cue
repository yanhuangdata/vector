package metadata

components: sinks: gcp_pubsub: {
	title: "GCP PubSub"

	classes: {
		commonly_used: true
		delivery:      "at_least_once"
		development:   "beta"
		egress_method: "batch"
		service_providers: ["GCP"]
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
				max_events:   1000
				timeout_secs: 1.0
			}
			compression: enabled: false
			encoding: {
				enabled: true
				codec: enabled: false
			}
			proxy: enabled: true
			request: {
				enabled: true
				headers: false
			}
			tls: {
				enabled:                true
				can_verify_certificate: true
				can_verify_hostname:    true
				enabled_default:        false
			}
			to: {
				service: services.gcp_pubsub

				interface: {
					socket: {
						api: {
							title: "GCP XML Interface"
							url:   urls.gcp_xml_interface
						}
						direction: "outgoing"
						protocols: ["http"]
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
		api_key:          configuration._gcp_api_key
		credentials_path: configuration._gcp_credentials_path
		endpoint: {
			common:      false
			description: "The endpoint to which to send data."
			required:    false
			type: string: {
				default: "https://pubsub.googleapis.com"
				examples: ["https://us-central1-pubsub.googleapis.com"]
			}
		}
		project: {
			description: "The project name to which to publish logs."
			required:    true
			type: string: {
				examples: ["vector-123456"]
			}
		}
		topic: {
			description: "The topic within the project to which to publish logs."
			required:    true
			type: string: {
				examples: ["this-is-a-topic"]
			}
		}
	}

	input: {
		logs:    true
		metrics: null
		traces:  false
	}

	permissions: iam: [
		{
			platform: "gcp"
			_service: "pubsub"

			policies: [
				{
					_action: "topics.get"
					required_for: ["healthcheck"]
				},
				{
					_action: "topics.publish"
					required_for: ["operation"]
				},
			]
		},
	]

	telemetry: metrics: {
		component_sent_bytes_total:       components.sources.internal_metrics.output.metrics.component_sent_bytes_total
		component_sent_events_total:      components.sources.internal_metrics.output.metrics.component_sent_events_total
		component_sent_event_bytes_total: components.sources.internal_metrics.output.metrics.component_sent_event_bytes_total
		events_out_total:                 components.sources.internal_metrics.output.metrics.events_out_total
	}
}
