package metadata

components: sinks: new_relic_logs: {
	title: "New Relic Logs"

	classes: {
		commonly_used: false
		delivery:      "at_least_once"
		development:   "deprecated"
		egress_method: "batch"
		service_providers: ["New Relic"]
		stateful: false
	}

	features: {
		acknowledgements: true
		healthcheck: enabled: true
		send: {
			batch: {
				enabled:      true
				common:       false
				max_bytes:    1_000_000
				timeout_secs: 1.0
			}
			compression: {
				enabled: true
				default: "none"
				algorithms: ["gzip"]
				levels: ["none", "fast", "default", "best", 0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
			}
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
				service: services.new_relic_logs

				interface: {
					socket: {
						api: {
							title: "New Relic  Log API"
							url:   urls.new_relic_log_api
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
		warnings: [
			"""
				This transform has been deprecated. Please use the [`new_relic` sink](\(urls.vector_new_relic_sink)) instead.
				""",
		]
		notices: []
	}

	configuration: {
		insert_key: {
			common:      true
			description: "Your New Relic insert key (if applicable)."
			required:    false
			type: string: {
				default: null
				examples: ["xxxx", "${NEW_RELIC_INSERT_KEY}"]
			}
		}
		license_key: {
			common:      true
			description: "Your New Relic license key (if applicable)."
			required:    false
			type: string: {
				default: null
				examples: ["xxxx", "${NEW_RELIC_LICENSE_KEY}"]
			}
		}
		region: {
			common:      true
			description: "The region to send data to."
			required:    false
			type: string: {
				default: "us"
				enum: {
					us: "United States"
					eu: "Europe"
				}
			}
		}

	}

	input: {
		logs:    true
		metrics: null
		traces:  false
	}

	telemetry: components.sinks.http.telemetry
}
