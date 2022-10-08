package metadata

components: transforms: split: {
	title: "Split"

	description: """
		Splits a string field on a defined separator.
		"""

	classes: {
		commonly_used: false
		development:   "deprecated"
		egress_method: "stream"
		stateful:      false
	}

	features: {
		shape: {}
	}

	support: {
		requirements: []
		warnings: [
			"""
			\(split._remap_deprecation_notice)

			```coffee
			.message = split(.message)
			```
			""",
		]
		notices: []
	}

	configuration: {
		drop_field: {
			common:      true
			description: "If `true` the `field` will be dropped after parsing."
			required:    false
			type: bool: default: true
		}
		field: {
			common:      true
			description: "The field to apply the split on."
			required:    false
			type: string: {
				default: "message"
				examples: ["message", "parent.child"]
			}
		}
		field_names: {
			description: "The field names assigned to the resulting tokens, in order."
			required:    true
			type: array: items: type: string: {
				examples: ["timestamp", "level", "message", "parent.child"]
			}
		}
		separator: {
			common:      true
			description: "The separator to split the field on. If no separator is given, it will split on all whitespace. 'Whitespace' is defined according to the terms of the [Unicode Derived Core Property `White_Space`](\(urls.unicode_whitespace))."
			required:    false
			type: string: {
				default: "[whitespace]"
				examples: [","]
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

	examples: [
		{
			title: "Split log message"
			configuration: {
				field:     "message"
				separator: ","
				field_names: ["remote_addr", "user_id", "timestamp", "message", "status", "bytes"]
				types: {
					status: "int"
					bytes:  "int"
				}
			}
			input: log: {
				message: "5.86.210.12,zieme4647,19/06/2019:17:20:49 -0400,GET /embrace/supply-chains/dynamic/vertical,201,20574"
			}
			output: log: {
				remote_addr: "5.86.210.12"
				user_id:     "zieme4647"
				timestamp:   "19/06/2019:17:20:49 -0400"
				message:     "GET /embrace/supply-chains/dynamic/vertical"
				status:      201
				bytes:       20574
			}
		},
	]

	telemetry: metrics: {
		processing_errors_total: components.sources.internal_metrics.output.metrics.processing_errors_total
	}
}
