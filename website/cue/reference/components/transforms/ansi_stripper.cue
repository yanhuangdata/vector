package metadata

components: transforms: ansi_stripper: {
	title: "ANSI Stripper"

	description: """
		Strips [ANSI escape sequences](\(urls.ansi_escape_codes)).
		"""

	classes: {
		commonly_used: false
		development:   "deprecated"
		egress_method: "stream"
		stateful:      false
	}

	features: {
		sanitize: {}
	}

	support: {
		requirements: []
		warnings: [
			"""
			\(ansi_stripper._remap_deprecation_notice)

			```coffee
			.message = strip_ansi_escape_codes(.message)
			```
			""",
		]
		notices: []
	}

	configuration: {
		field: {
			common:      true
			description: "The target field to strip ANSI escape sequences from."
			required:    false
			type: string: {
				default: "message"
				examples: ["message", "parent.child", "array[0]"]
			}
		}
	}

	input: {
		logs:    true
		metrics: null
		traces:  false
	}

	telemetry: metrics: {
		processing_errors_total: components.sources.internal_metrics.output.metrics.processing_errors_total
	}
}
