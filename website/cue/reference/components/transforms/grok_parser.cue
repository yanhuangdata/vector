package metadata

components: transforms: grok_parser: {
	title: "Grok Parser"

	description: """
		Parses a log field value with [Grok](\(urls.grok)).
		"""

	classes: {
		commonly_used: false
		development:   "deprecated"
		egress_method: "stream"
		stateful:      false
	}

	features: {
		parse: {
			format: {
				name:     "Grok"
				url:      urls.grok
				versions: null
			}
		}
	}

	support: {
		requirements: []
		warnings: [
			"""
			\(grok_parser._remap_deprecation_notice)

			```coffee
			.message = parse_grok(.message, "%{TIMESTAMP_ISO8601:timestamp} %{LOGLEVEL:level} %{GREEDYDATA:message}")
			```
			""",
		]
		notices: [
			"""
				Vector uses the Rust [`grok` library](\(urls.rust_grok_library)). All patterns
				[listed here](\(urls.grok_patterns)) are supported. It is recommended to use
				maintained patterns when possible since they will be improved over time by
				the community.
				""",
		]
	}

	configuration: {
		drop_field: {
			common:      true
			description: "If `true` will drop the specified `field` after parsing."
			required:    false
			type: bool: default: true
		}
		field: {
			common:      true
			description: "The log field to execute the `pattern` against. Must be a `string` value."
			required:    false
			type: string: {
				default: "message"
				examples: ["message", "parent.child", "array[0]"]
			}
		}
		pattern: {
			description: "The [Grok pattern](\(urls.grok_patterns))"
			required:    true
			type: string: {
				examples: ["%{TIMESTAMP_ISO8601:timestamp} %{LOGLEVEL:level} %{GREEDYDATA:message}"]
			}
		}
		timezone: configuration._timezone
		types:    configuration._types
	}

	input: {
		logs:    true
		metrics: null
		traces:  false
	}

	how_it_works: {
		available_patterns: {
			title: "Available Patterns"
			body:  support.notices[0]
		}

		testing: {
			title: "Testing"
			body:  """
				We recommend the [Grok debugger](\(urls.grok_debugger)) for Grok testing.
				"""
		}
	}

	telemetry: metrics: {
		processing_errors_total: components.sources.internal_metrics.output.metrics.processing_errors_total
	}
}
