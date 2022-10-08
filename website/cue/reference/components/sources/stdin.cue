package metadata

components: sources: stdin: {
	title: "STDIN"

	classes: {
		commonly_used: false
		delivery:      "at_least_once"
		deployment_roles: ["sidecar"]
		development:   "stable"
		egress_method: "stream"
		stateful:      false
	}

	features: {
		acknowledgements: false
		multiline: enabled: false
		codecs: {
			enabled:         true
			default_framing: "`newline_delimited` for codecs other than `native`, which defaults to `length_delimited`"
		}
		receive: {
			from: {
				service: services.stdin
				interface: stdin: {}
			}

			tls: enabled: false
		}
	}

	support: {
		requirements: []
		warnings: []
		notices: []
	}

	installation: {
		platform_name: null
	}

	configuration: {
		host_key: {
			category:    "Context"
			common:      false
			description: """
				The key name added to each event representing the current host. This can also be globally set via the
				[global `host_key` option](\(urls.vector_configuration)/global-options#log_schema.host_key).
				"""
			required:    false
			type: string: {
				default: "host"
			}
		}
		max_length: {
			common:      false
			description: "The maximum bytes size of a message before rest of it will be discarded."
			required:    false
			type: uint: {
				default: 102400
				unit:    "bytes"
			}
		}
	}

	output: logs: line: {
		description: "An individual event from STDIN."
		fields: {
			host:      fields._local_host
			message:   fields._raw_line
			timestamp: fields._current_timestamp
		}
	}

	examples: [
		{
			_line: """
				2019-02-13T19:48:34+00:00 [info] Started GET "/" for 127.0.0.1
				"""
			title: "STDIN line"
			configuration: {}
			input: _line
			output: log: {
				timestamp: _values.current_timestamp
				message:   _line
				host:      _values.local_host
			}
		},
	]

	how_it_works: {
		line_delimiters: {
			title: "Line Delimiters"
			body: """
				Each line is read until a new line delimiter, the `0xA` byte, is found.
				"""
		}
	}

	telemetry: metrics: {
		events_in_total:                      components.sources.internal_metrics.output.metrics.events_in_total
		processed_bytes_total:                components.sources.internal_metrics.output.metrics.processed_bytes_total
		processed_events_total:               components.sources.internal_metrics.output.metrics.processed_events_total
		component_discarded_events_total:     components.sources.internal_metrics.output.metrics.component_discarded_events_total
		component_errors_total:               components.sources.internal_metrics.output.metrics.component_errors_total
		component_received_bytes_total:       components.sources.internal_metrics.output.metrics.component_received_bytes_total
		component_received_event_bytes_total: components.sources.internal_metrics.output.metrics.component_received_event_bytes_total
		component_received_events_total:      components.sources.internal_metrics.output.metrics.component_received_events_total
		stdin_reads_failed_total:             components.sources.internal_metrics.output.metrics.stdin_reads_failed_total
	}
}
