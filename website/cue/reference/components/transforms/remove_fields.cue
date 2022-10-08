package metadata

components: transforms: remove_fields: {
	title: "Remove Fields"

	description: """
		Removes one or more log fields.
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
			\(remove_fields._remap_deprecation_notice)

			```coffee
			del(.level)
			```
			""",
		]
		notices: []
	}

	configuration: {
		drop_empty: {
			common:      false
			description: "If set to `true`, after removing fields, remove any parent objects that are now empty."
			required:    false
			type: bool: default: false
		}
		fields: {
			description: "The log field names to drop."
			required:    true
			type: array: items: type: string: {
				examples: ["field1", "field2", "parent.child"]
			}
		}
	}

	input: {
		logs:    true
		metrics: null
		traces:  false
	}
}
