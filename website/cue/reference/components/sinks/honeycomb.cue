package metadata

components: sinks: honeycomb: {
	title: "Honeycomb"

	classes: {
		commonly_used: false
		delivery:      "at_least_once"
		development:   "beta"
		egress_method: "batch"
		service_providers: ["Honeycomb"]
		stateful: false
	}

	features: {
		acknowledgements: true
		healthcheck: enabled: true
		send: {
			batch: {
				enabled:      true
				common:       false
				max_bytes:    100_000
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
			tls: enabled: false
			to: {
				service: services.honeycomb

				interface: {
					socket: {
						api: {
							title: "Honeycomb batch events API"
							url:   urls.honeycomb_batch
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
		api_key: {
			description: "The team key that will be used to authenticate against Honeycomb."
			required:    true
			type: string: {
				examples: ["${HONEYCOMB_API_KEY}", "some-api-key"]
			}
		}
		dataset: {
			description: "The dataset that Vector will send logs to."
			required:    true
			type: string: {
				examples: ["my-honeycomb-dataset"]
			}
		}
	}

	input: {
		logs:    true
		metrics: null
		traces:  false
	}

	how_it_works: {
		setup: {
			title: "Setup"
			body:  """
				1. Register for a free account at [honeycomb.io](\(urls.honeycomb_signup))

				2. Once registered, create a new dataset and when presented with log shippers select the
				curl option and use the key provided with the curl example.
				"""
		}
	}

	telemetry: metrics: {
		component_sent_bytes_total:       components.sources.internal_metrics.output.metrics.component_sent_bytes_total
		component_sent_events_total:      components.sources.internal_metrics.output.metrics.component_sent_events_total
		component_sent_event_bytes_total: components.sources.internal_metrics.output.metrics.component_sent_event_bytes_total
		events_out_total:                 components.sources.internal_metrics.output.metrics.events_out_total
	}
}
